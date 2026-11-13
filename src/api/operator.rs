//! API-4/5/27/48/64/68, WIR-34/35/51/55: isolated operator authority.
use super::*;

pub(super) struct Authority {
    pub address: SocketAddr,
    principals: Vec<(String, String, bool)>,
}
impl Authority {
    pub async fn exclude_customer_credentials(&self, store: &Store) -> Result<()> {
        let digests: Vec<String> = self
            .principals
            .iter()
            .map(|(_, digest, _)| digest.clone())
            .collect();
        let collision=store.value("SELECT to_json(EXISTS(SELECT 1 FROM provisiond.tenants WHERE credential_digest=ANY($1::text[]) OR recovery_digest=ANY($1::text[])))::text",&[&digests]).await?;
        ensure!(
            collision != true,
            "operator authority must not contain a customer credential"
        );
        Ok(())
    }
    pub fn load() -> Result<Option<Self>> {
        let Some(address) = std::env::var("PROVISIOND_OPERATOR_LISTEN").ok() else {
            return Ok(None);
        };
        let address = http::loopback(&address)?;
        let path = std::env::var("PROVISIOND_OPERATOR_AUTHORITY")?;
        let config: Value = serde_json::from_slice(&std::fs::read(path)?)?;
        let mut principals = Vec::new();
        let mut names = std::collections::BTreeSet::new();
        let mut digests = std::collections::BTreeSet::new();
        for entry in config
            .as_array()
            .context("operator authority must be a list")?
        {
            ensure!(
                entry.as_object().is_some_and(|o| o.len() == 3),
                "operator authority fields"
            );
            let principal = entry["principal"]
                .as_str()
                .filter(|s| opaque(s))
                .context("opaque operator principal required")?;
            let token = std::env::var(
                entry["token_env"]
                    .as_str()
                    .context("token environment name")?,
            )?;
            ensure!(token.len() >= 24, "operator token too short");
            let digest = wire::digest(token.as_bytes());
            ensure!(
                names.insert(principal.to_owned()) && digests.insert(digest.clone()),
                "duplicate operator principal or token"
            );
            principals.push((
                principal.into(),
                digest,
                entry["admin"].as_bool().context("operator admin flag")?,
            ));
        }
        ensure!(!principals.is_empty(), "empty operator authority");
        Ok(Some(Self {
            address,
            principals,
        }))
    }
    fn authenticate(&self, r: &Request) -> Option<(&str, bool)> {
        let token = r.headers.get("authorization")?.strip_prefix("Bearer ")?;
        let digest = wire::digest(token.as_bytes());
        self.principals
            .iter()
            .find(|(_, expected, _)| bool::from(expected.as_bytes().ct_eq(digest.as_bytes())))
            .map(|(id, _, admin)| (id.as_str(), *admin))
    }
}
fn opaque(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
}
fn invalid(field: &str) -> Error {
    Error::new(
        "invalid_request",
        "invalid resolution field",
        false,
        json!({"field":field}),
    )
}
impl State {
    pub(super) async fn operator_handle(
        &self,
        r: &Request,
        correlation: &str,
    ) -> Result<(u16, Value)> {
        let authority = self
            .operators
            .as_ref()
            .context("operator listener authority")?;
        let Some((principal, admin)) = authority.authenticate(r) else {
            if self.authenticate(r).await.is_ok() {
                return Err(Error::missing().into());
            }
            return Err(Error::new(
                "authentication",
                "operator authentication required",
                false,
                json!({"reason":"token"}),
            )
            .into());
        };
        // SEC-32/39: authentication is known here, before rate, override,
        // target, key or body validation can refuse a supported mutating verb.
        let parts: Vec<_> = r
            .target
            .split('?')
            .next()
            .unwrap_or(&r.target)
            .split('/')
            .collect();
        let supported = match (r.method.as_str(), parts.as_slice()) {
            ("POST", ["", "v1", "operations", id, "actions", "resolve"]) => {
                Some(("operation", *id, "resolve"))
            }
            (
                "POST",
                [
                    "",
                    "v1",
                    "episodes",
                    id,
                    "actions",
                    verb @ ("keep" | "retry"),
                ],
            ) => Some(("episode", *id, *verb)),
            _ => None,
        };
        let mut early_audit = supported.map(|(entity, id, verb)| {
            let target = uuid::Uuid::parse_str(id).ok().map(|_| id);
            let tenant_override = if admin {
                r.headers.get("x-provisiond-tenant").filter(|value| uuid::Uuid::parse_str(value).is_ok())
            } else { None };
            json!({"event":"operator_decision","correlation_id":correlation,
                "principal":principal,"tenant":null,"admin_override":tenant_override,
                "target":target,"target_kind":entity,"verb":verb,"reason":null,
                "outcome":"refused","operation_id":if entity=="operation" {json!(target)} else {Value::Null},
                "operation_kind":if entity=="episode" {json!("delete")} else {Value::Null},
                "machine_id":null,"prior_operation_id":null,"resolution":null})
        });
        let mut decision_started = false;
        let result = self
            .operator_authenticated(
                r,
                correlation,
                principal,
                admin,
                &mut decision_started,
                &mut early_audit,
            )
            .await;
        if !decision_started {
            if let (Some(mut audit), Err(error)) = (early_audit, &result) {
                let reason = match error
                    .downcast_ref::<Error>()
                    .and_then(|error| error.0["kind"].as_str())
                {
                    Some("invalid_request") => "invalid_request",
                    Some("not_found") => "not_found",
                    Some("rate_limited") => "rate_limited",
                    Some("overloaded") => "overloaded",
                    _ => "request_refused",
                };
                audit["reason"] = json!(reason);
                (self.trace)(audit);
            }
        }
        result
    }
    async fn operator_authenticated(
        &self,
        r: &Request,
        correlation: &str,
        principal: &str,
        admin: bool,
        decision_started: &mut bool,
        early_audit: &mut Option<Value>,
    ) -> Result<(u16, Value)> {
        self.rate(&format!("operator:{principal}"), r.method != "GET")?;
        let tenant_override = if admin {
            r.headers.get("x-provisiond-tenant").map(String::as_str)
        } else {
            None
        };
        if let Some(tenant) = tenant_override {
            if uuid::Uuid::parse_str(tenant).is_err() {
                return Err(invalid("/tenant").into());
            }
        }
        let path = r.target.split('?').next().unwrap_or(&r.target);
        let parts: Vec<_> = path.split('/').collect();
        if r.method == "GET" {
            return match parts.as_slice() {
                ["", "v1", "operations"] => {
                    let scope = json!(["operator", principal, tenant_override]).to_string();
                    let page = self
                        .scoped_page(r, tenant_override, &scope, "operations")
                        .await?;
                    Ok((
                        200,
                        json!({"operations":page["items"],"next_cursor":page["next_cursor"],"poll_after_ms":1000}),
                    ))
                }
                ["", "v1", "episodes"] => {
                    Ok((200, self.episode_page(r, principal, tenant_override).await?))
                }
                ["", "v1", "episodes", id] => Ok((200, self.episode(id, tenant_override).await?)),
                ["", "v1", "operations", id] => {
                    // WIR-27/51: the same receipt horizon and retained identities
                    // as customer detail, with operator effective-tenant authority.
                    let history = operation_history(
                        "candidate.id::text=$1",
                        "i.resource_id::text=$1",
                        self.p.n("retention_seconds"),
                    );
                    let sql = format!(
                        "SELECT COALESCE((SELECT jsonb_build_object('body',h.body,'expired',h.expired,'retained_until',provisiond.wire_time(h.expires_at)) FROM {history} h WHERE h.id::text=$1 AND ($2::text IS NULL OR h.tenant_id=$2 OR (h.tenant_id IS NULL AND EXISTS(SELECT 1 FROM provisiond.idempotency_records r WHERE r.resource_kind='operation' AND r.resource_id=h.id AND r.scope_kind='tenant' AND r.scope_id=$2) OR h.tenant_id IS NULL AND EXISTS(SELECT 1 FROM provisiond.episodes e JOIN provisiond.machines m ON m.id=e.machine_id WHERE e.current_operation_id=h.id AND m.tenant_id=$2)))),'null'::jsonb)::text"
                    );
                    Ok((
                        200,
                        operation_result(&self.store.value(&sql, &[id, &tenant_override]).await?)?,
                    ))
                }
                _ => Err(unsupported().into()),
            };
        }
        let (entity, id, verb) = match (r.method.as_str(), parts.as_slice()) {
            ("POST", ["", "v1", "operations", id, "actions", "resolve"]) => {
                ("operation", *id, "resolve")
            }
            (
                "POST",
                [
                    "",
                    "v1",
                    "episodes",
                    id,
                    "actions",
                    verb @ ("keep" | "retry"),
                ],
            ) => ("episode", *id, *verb),
            _ => return Err(unsupported().into()),
        };
        let key = r
            .headers
            .get("idempotency-key")
            .filter(|key| {
                (8..=200).contains(&key.len())
                    && key
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
            })
            .ok_or_else(|| invalid("/idempotency_key"))?;
        let record = if entity == "operation" {
            self.store.value("SELECT COALESCE((SELECT to_jsonb(o) FROM provisiond.operations o WHERE id::text=$1 AND ($2::text IS NULL OR tenant_id=$2)),'null')::text",&[&id,&tenant_override]).await?
        } else {
            self.store.value("SELECT COALESCE((SELECT to_jsonb(e)||jsonb_build_object('tenant_id',m.tenant_id,'provider_account',m.provider_account) FROM provisiond.episodes e JOIN provisiond.machines m ON m.id=e.machine_id WHERE e.id::text=$1 AND ($2::text IS NULL OR m.tenant_id=$2)),'null')::text",&[&id,&tenant_override]).await?
        };
        if record.is_null() {
            return Err(Error::missing().into());
        }
        let tenant = record["tenant_id"].as_str().context("operation tenant")?;
        // Scoped target lookup has succeeded. These facts are safe to retain
        // even if parsing or verb-field validation now refuses the request.
        if let Some(audit) = early_audit {
            audit["tenant"] = json!(tenant);
            audit["machine_id"] = record["machine_id"].clone();
            audit["prior_operation_id"] = record["current_operation_id"].clone();
            if entity == "operation" {
                audit["operation_kind"] = record["kind"].clone();
            }
        }
        let body = wire::parse(&r.body)?;
        if verb == "resolve" {
            let resolution = body["resolution"].as_str().unwrap_or("");
            let create = record["kind"] == "create";
            if !(create && matches!(resolution, "observed" | "absent" | "abandoned")
                || record["kind"] == "delete"
                    && matches!(resolution, "applied" | "not_applied" | "abandoned"))
            {
                return Err(invalid("/resolution").into());
            }
            let mut bad = wire::unknown(
                &body,
                if resolution == "observed" {
                    &[
                        "resolution",
                        "external_id",
                        "kept_duplicate",
                        "operator_ref",
                    ]
                } else {
                    &["resolution", "operator_ref"]
                },
            );
            if resolution == "observed" && !body["external_id"].as_str().is_some_and(opaque) {
                bad.push("/external_id".into());
            }
            if body.get("kept_duplicate").is_some()
                && !body["kept_duplicate"].as_str().is_some_and(opaque)
            {
                bad.push("/kept_duplicate".into());
            }
            if !body["operator_ref"].as_str().is_some_and(opaque) {
                bad.push("/operator_ref".into());
            }
            wire::least(bad)?;
        } else if verb == "keep" {
            let mut bad = wire::unknown(&body, &["operator_ref"]);
            if !body["operator_ref"].as_str().is_some_and(opaque) {
                bad.push("/operator_ref".into());
            }
            wire::least(bad)?;
        } else {
            let mut bad = wire::unknown(&body, &["reason"]);
            if !body["reason"]
                .as_str()
                .is_some_and(|s| !s.is_empty() && s.len() <= 1024)
            {
                bad.push("/reason".into());
            }
            wire::least(bad)?;
        }
        // SEC-32/39: log safe authenticated intent separately from the observed
        // commit result. Free-form retry prose is represented only by its digest.
        let reason = if verb == "retry" {
            format!(
                "sha256:{}",
                wire::digest(
                    body["reason"]
                        .as_str()
                        .context("validated reason")?
                        .as_bytes()
                )
            )
        } else {
            body["operator_ref"]
                .as_str()
                .context("validated evidence reference")?
                .to_owned()
        };
        let audit = json!({"event":"operator_decision","correlation_id":correlation,
            "principal":principal,"tenant":tenant,"admin_override":tenant_override,
            "target":id,"target_kind":entity,"verb":verb,"reason":reason,
            "resolution":body["resolution"],"operation_id":if entity=="operation" {json!(id)} else {Value::Null},
            "prior_operation_id":record["current_operation_id"],
            "operation_kind":if entity=="operation" {record["kind"].clone()} else {json!("delete")},"machine_id":record["machine_id"]});
        *decision_started = true;
        let mut intent = audit.clone();
        intent["outcome"] = json!("intent");
        (self.trace)(intent);
        let mut observed_outcome = "refused";
        let result: Result<(u16,Value)> = async {
        let fingerprint = wire::fingerprint(&r.method, &r.target, &body);
        let component = self.store.component();
        let mut tx = component.begin().await?;
        // API-10: one principal owns a key across all target tenants/routes.
        tx.rows(
            "SELECT pg_advisory_xact_lock(1347572817,hashtext($1))",
            &[&principal],
        )
        .await?;
        let prior=tx.rows("SELECT fingerprint,status,response_body::text,expires_at<=transaction_timestamp() FROM provisiond.idempotency_records WHERE scope_kind='operator' AND scope_id=$1 AND key=$2",&[&principal,&key]).await?;
        if let Some(prior) = prior.first() {
            if prior.get::<_, String>(0) != fingerprint {
                return Err(Error::conflict("idempotency_mismatch").into());
            }
            if prior.get::<_, bool>(3) {
                return Err(Error::new("gone", "receipt retention ended", false, json!({})).into());
            }
            let response = (
                prior.get::<_, i32>(1) as u16,
                serde_json::from_str(&prior.get::<_, String>(2))?,
            );
            tx.rollback().await?;
            observed_outcome="replayed";
            return Ok(response);
        }
        tx.serialize_tenants(&[tenant]).await?;
        if verb == "resolve" {
            // API-7 step 5c / SEC-39: replay has already returned; the principal
            // lock covers this allowance across every effective tenant.
            self.ceiling(&mut tx,&format!("operator:{principal}"),"operator_resolutions",1,
                self.p.value["operator_resolution_limit"].as_i64().unwrap_or(self.p.n("created_limit")),false).await?;
            let verb = body["resolution"].as_str().context("validated verb")?;
            let resolution = crate::store::Resolution {
                id: id.into(),
                tenant: tenant.into(),
                verb: verb.into(),
                resolver: principal.into(),
                automatic: false,
                evidence: body.clone(),
                machine: json!({"external_id":body["external_id"],"state":"unknown","public_ips":[]}),
                earliest_date: None,
                observed_at: chrono::Utc::now(),
            };
            if !tx.resolve(&resolution).await? {
                return Err(Error::conflict("state").into());
            }
            self.ceiling(&mut tx,&format!("operator:{principal}"),"operator_resolutions",1,
                self.p.value["operator_resolution_limit"].as_i64().unwrap_or(self.p.n("created_limit")),true).await?;
        } else {
            if verb == "retry" {
                self.ceiling(
                    &mut tx,
                    &format!("operator:{principal}"),
                    "operator_retries",
                    1,
                    self.p.value["operator_retry_limit"]
                        .as_i64()
                        .unwrap_or(self.p.n("created_limit")),
                    false,
                )
                .await?;
                if !self
                    .catalog
                    .get(
                        record["provider_account"]
                            .as_str()
                            .context("retry account")?,
                    )
                    .is_some_and(|d| pricing::has(d, "delete_machine"))
                {
                    return Err(unsupported().into());
                }
            }
            tx.episode_action(id, tenant, verb, principal, &body)
                .await?;
            if verb == "retry" {
                self.ceiling(
                    &mut tx,
                    &format!("operator:{principal}"),
                    "operator_retries",
                    1,
                    self.p.value["operator_retry_limit"]
                        .as_i64()
                        .unwrap_or(self.p.n("created_limit")),
                    true,
                )
                .await?;
            }
        }
        let response: Value = if entity == "operation" {
            serde_json::from_str(
                &tx.rows(
                    "SELECT body::text FROM provisiond.operation_views WHERE id::text=$1",
                    &[&id],
                )
                .await?[0]
                    .get::<_, String>(0),
            )?
        } else {
            serde_json::from_str(
                &tx.rows(
                    "SELECT body::text FROM provisiond.episode_views WHERE id::text=$1",
                    &[&id],
                )
                .await?[0]
                    .get::<_, String>(0),
            )?
        };
        tx.rows("INSERT INTO provisiond.idempotency_records(scope_kind,scope_id,key,fingerprint,resource_kind,resource_id,status,response_body,expires_at) VALUES('operator',$1,$2,$3,$4,$5::text::uuid,200,$6::text::jsonb,transaction_timestamp()+$7::bigint*interval '1 second') RETURNING resource_id",&[&principal,&key,&fingerprint,&entity,&id,&response.to_string(),&self.p.n("receipt_seconds")]).await?;
        observed_outcome="uncertain";
        ensure!(
            component.commit(tx)?.wait().await? == CommitOutcome::Acknowledged,
            "operator decision unacknowledged; resend same key"
        );
        observed_outcome="acknowledged";
        Ok((200, response))
        }.await;
        let mut event = audit;
        event["outcome"] = json!(observed_outcome);
        if entity == "episode" {
            if let Ok((_, response)) = &result {
                event["operation_id"] = response["current_operation_id"].clone();
            }
        }
        event["error_kind"] = json!(
            result
                .as_ref()
                .err()
                .and_then(|error| error.downcast_ref::<Error>())
                .map(|error| error.0["kind"].clone())
        );
        (self.trace)(event);
        result
    }
    async fn episode(&self, id: &str, tenant: Option<&str>) -> Result<Value> {
        let result=self.store.value("SELECT COALESCE((SELECT body FROM provisiond.episode_views WHERE id::text=$1 AND ($2::text IS NULL OR tenant_id=$2)),'null')::text",&[&id,&tenant]).await?;
        if result.is_null() {
            return Err(Error::missing().into());
        }
        Ok(result)
    }
    async fn episode_page(
        &self,
        r: &Request,
        principal: &str,
        tenant: Option<&str>,
    ) -> Result<Value> {
        let query: BTreeMap<_, _> = ["state", "limit", "cursor"]
            .into_iter()
            .filter_map(|key| super::query(&r.target, key).map(|v| (key, v)))
            .collect();
        let states = query
            .get("state")
            .map(|s| s.split(',').collect::<Vec<_>>())
            .unwrap_or_else(|| vec!["attempting", "uncertain", "stalled", "scheduled"]);
        if states.iter().any(|s| {
            !matches!(
                *s,
                "attempting" | "uncertain" | "stalled" | "scheduled" | "closed"
            )
        }) {
            return Err(invalid("/state").into());
        }
        let limit = query
            .get("limit")
            .map(|s| s.parse::<i64>())
            .transpose()
            .map_err(|_| invalid("/limit"))?
            .unwrap_or(50)
            .clamp(1, 200);
        let scope = json!([principal, tenant, states]).to_string();
        let cursor = match query.get("cursor") {
            Some(token) => Some(
                self.admission
                    .open(token)
                    .and_then(|v| serde_json::from_str::<Value>(&v).ok())
                    .filter(|v| v["scope"] == scope)
                    .ok_or_else(|| invalid("/cursor"))?,
            ),
            None => None,
        };
        let date = cursor.as_ref().and_then(|c| c["opened_at"].as_str());
        let id = cursor.as_ref().and_then(|c| c["id"].as_str());
        let states: Vec<String> = states.into_iter().map(str::to_owned).collect();
        let rows=self.store.value("SELECT COALESCE(jsonb_agg(to_jsonb(v) ORDER BY opened_at DESC,id),'[]')::text FROM (SELECT * FROM provisiond.episode_views WHERE state=ANY($1::text[]) AND ($2::text IS NULL OR tenant_id=$2) AND ($3::text IS NULL OR (opened_at<$3::text::timestamptz OR (opened_at=$3::text::timestamptz AND id::text>$4))) ORDER BY opened_at DESC,id LIMIT $5) v",&[&states,&tenant,&date,&id,&(limit+1)]).await?;
        let mut rows = rows.as_array().context("episode page")?.clone();
        let more = rows.len() > limit as usize;
        rows.truncate(limit as usize);
        let next = if more {
            rows.last().map(|last| {
                self.admission.seal(
                    &json!({"scope":scope,"opened_at":last["opened_at"],"id":last["id"]})
                        .to_string(),
                )
            })
        } else {
            None
        };
        Ok(
            json!({"episodes":rows.into_iter().map(|v|v["body"].clone()).collect::<Vec<_>>(),"next_cursor":next}),
        )
    }
    pub(super) async fn delete(
        &self,
        tx: &mut Transaction,
        tenant: &str,
        machine: &str,
        request: &Request,
        body: &Value,
        correlation: &str,
    ) -> Result<(u16, Value)> {
        let key = request
            .headers
            .get("idempotency-key")
            .context("validated idempotency key")?;
        let fingerprint = wire::fingerprint(&request.method, &request.target, body);
        self.ceiling(
            tx,
            tenant,
            "machines_destroyed",
            1,
            self.p.value["destroyed_limit"]
                .as_i64()
                .unwrap_or(self.p.n("created_limit")),
            false,
        )
        .await?;
        let row=tx.rows("SELECT provider_account,external_id FROM provisiond.machines WHERE id::text=$1 AND tenant_id=$2",&[&machine,&tenant]).await?;
        let row = row.first().ok_or_else(Error::missing)?;
        let account: String = row.get(0);
        let external: String = row.get(1);
        if !self
            .catalog
            .get(&account)
            .is_some_and(|d| pricing::has(d, "delete_machine"))
        {
            return Err(unsupported().into());
        }
        // OPS-2: caller payload lives through execution, then settlement purges it.
        let id:String=tx.rows("INSERT INTO provisiond.operations(tenant_id,idempotency_key,kind,status,machine_id,provider_account,request,request_summary,correlation_id) VALUES($1,$2,'delete','queued',$3::text::uuid,$4,$7::text::jsonb,jsonb_build_object('kind','delete','machine_id',$3::text,'provider_account',$4::text,'external_id',$5::text),$6) RETURNING id::text",&[&tenant,&key,&machine,&account,&external,&correlation,&body.to_string()]).await?[0].get(0);
        self.ceiling(
            tx,
            tenant,
            "machines_destroyed",
            1,
            self.p.value["destroyed_limit"]
                .as_i64()
                .unwrap_or(self.p.n("created_limit")),
            true,
        )
        .await?;
        let response: Value = serde_json::from_str(
            &tx.rows(
                "SELECT body::text FROM provisiond.operation_views WHERE id::text=$1",
                &[&id],
            )
            .await?[0]
                .get::<_, String>(0),
        )?;
        tx.rows("INSERT INTO provisiond.idempotency_records(scope_kind,scope_id,key,fingerprint,resource_kind,resource_id,status,response_body,expires_at) VALUES('tenant',$1,$2,$3,'operation',$4::text::uuid,202,$5::text::jsonb,transaction_timestamp()+$6::bigint*interval '1 second') RETURNING resource_id",&[&tenant,&key,&fingerprint,&id,&response.to_string(),&self.p.n("receipt_seconds")]).await?;
        Ok((202, response))
    }
}
