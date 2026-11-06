//! Loopback-only bounded HTTP/1.1 transport for the explicit local profile.
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::{collections::BTreeMap, net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
pub struct Request {
    pub method: String,
    pub target: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}
pub fn loopback(value: &str) -> Result<SocketAddr> {
    let a: SocketAddr = value.parse()?;
    ensure!(
        a.ip().is_loopback() && a.port() > 0,
        "listener must be loopback with an explicit port"
    );
    Ok(a)
}
pub async fn read(stream: &mut TcpStream) -> Result<Request> {
    let mut head = Vec::new();
    let mut b = [0];
    while !head.ends_with(b"\r\n\r\n") {
        ensure!(head.len() < 16384, "HTTP headers too large");
        ensure!(stream.read(&mut b).await? == 1, "incomplete HTTP headers");
        head.push(b[0]);
    }
    let text = std::str::from_utf8(&head)?;
    let mut lines = text.split("\r\n");
    let mut start = lines.next().context("request line")?.split(' ');
    let method = start.next().context("method")?.to_owned();
    let target = start.next().context("target")?.to_owned();
    ensure!(
        start.next() == Some("HTTP/1.1")
            && start.next().is_none()
            && target.starts_with('/')
            && !target.contains('#'),
        "unsupported HTTP request line"
    );
    let mut headers = BTreeMap::new();
    for line in lines.filter(|s| !s.is_empty()) {
        let (k, v) = line.split_once(':').context("header")?;
        let k = k.to_ascii_lowercase();
        ensure!(
            headers.insert(k, v.trim().to_owned()).is_none(),
            "duplicate HTTP header"
        );
    }
    ensure!(
        !headers.contains_key("transfer-encoding"),
        "chunked requests unsupported in local profile"
    );
    let len = headers
        .get("content-length")
        .map(|v| v.parse::<usize>())
        .transpose()?
        .unwrap_or(0);
    ensure!(len <= 2_097_152, "HTTP body too large");
    let mut body = vec![0; len];
    stream.read_exact(&mut body).await?;
    Ok(Request {
        method,
        target,
        headers,
        body,
    })
}
pub async fn respond(
    stream: &mut TcpStream,
    status: u16,
    body: &Value,
    headers: &[(String, String)],
) -> Result<()> {
    let data = body.to_string();
    let mut head = format!(
        "HTTP/1.1 {status} response\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n",
        data.len()
    );
    for (k, v) in headers {
        ensure!(
            !k.contains(['\r', '\n']) && !v.contains(['\r', '\n']),
            "invalid response header"
        );
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(data.as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}
/// Simulator transport owns no credential or transaction. Callers hold ExternalCall.
pub(crate) async fn call(
    address: SocketAddr,
    method: &str,
    path: &str,
    body: &Value,
    bound: Duration,
) -> Result<Value> {
    ensure!(address.ip().is_loopback(), "simulator must use loopback");
    tokio::time::timeout(bound,async{
  let mut s=TcpStream::connect(address).await?;let body=body.to_string();s.write_all(format!("{method} {path} HTTP/1.1\r\nHost: simulator\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await?;
  let mut bytes=Vec::new();s.take(2_097_153).read_to_end(&mut bytes).await?;ensure!(bytes.len()<=2_097_152,"simulator response too large");
  let at=bytes.windows(4).position(|w|w==b"\r\n\r\n").context("simulator reply absent")?;
  let header=std::str::from_utf8(&bytes[..at])?;ensure!(header.starts_with("HTTP/1.1 200 ")||header.starts_with("HTTP/1.0 200 "),"simulator refused request");
  Ok(serde_json::from_slice(&bytes[at+4..])?)
 }).await.context("simulator call deadline")?
}
