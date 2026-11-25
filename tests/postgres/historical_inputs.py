"""Verified, in-tree inputs for the two immutable W7 historical sources."""
import hashlib
import json
from pathlib import Path

BASE = '8339aae5ecf20e6337f4d5d5e22ec4f4573efbfb'
FIXTURE = '06b7636cfed13cd4d6049dbe316ed8b191c93fa2'
ROOT = Path(__file__).resolve().parent / 'fixtures' / 'w7'


def load_w7_inputs():
    """Return the verified bytes themselves, without Git or repair fallbacks."""
    manifest_path = ROOT / 'manifest.json'

    def read(path):
        if not path.resolve().is_relative_to(ROOT.resolve()):
            raise ValueError(f'W7 historical input unsafe path: {path}')
        try:
            return path.read_bytes()
        except FileNotFoundError as error:
            raise ValueError(f'W7 historical input missing: {path}') from error

    raw = read(manifest_path)
    try:
        manifest = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as error:
        raise ValueError(f'W7 historical input invalid manifest: {manifest_path}') from error

    def require(condition, detail):
        if not condition:
            raise ValueError(f'W7 historical input invalid manifest {manifest_path}: {detail}')

    require(isinstance(manifest, dict), 'expected an object')
    require(manifest.get('version') == 1, 'expected version 1')
    baseline = manifest.get('baseline')
    cli = manifest.get('cli')
    require(isinstance(baseline, dict) and isinstance(cli, dict), 'baseline and cli required')
    require(baseline.get('commit') == BASE, f'baseline commit must be {BASE}')
    require(cli.get('commit') == FIXTURE and cli.get('path') == 'tests/postgres/w7.py',
            f'cli source must be {FIXTURE}:tests/postgres/w7.py')
    entries = baseline.get('files')
    require(isinstance(entries, list) and len(entries) == 41, 'expected 41 baseline files')
    seen = set()

    def payload(entry, prefix):
        require(isinstance(entry, dict), f'{prefix} entry must be an object')
        path = entry.get('path')
        require(isinstance(path, str) and path and '\\' not in path
                and all(part not in ('', '.', '..') for part in path.split('/')),
                f'unsafe source path: {path!r}')
        if prefix == 'baseline':
            require(path.startswith(('src/', 'migrations/')) or path in ('Cargo.toml', 'Cargo.lock')
                    or path.startswith('tests/postgres/') and path.endswith('.rs'),
                    f'not a selected baseline source: {path}')
        relative = f'baseline/{path}' if prefix == 'baseline' else 'cli/w7.py'
        require(relative not in seen, f'duplicate entry: {relative}')
        seen.add(relative)
        data = read(ROOT / relative)
        digest = hashlib.sha256(data).hexdigest()
        blob = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
        if (type(entry.get('bytes')) is not int or len(data) != entry['bytes']
                or digest != entry.get('sha256') or blob != entry.get('git_blob')):
            raise ValueError(f'W7 historical input integrity error: {ROOT / relative} '
                             f'(bytes={len(data)}, sha256={digest})')
        return data

    sources = {}
    for entry in entries:
        data = payload(entry, 'baseline')
        sources[entry['path']] = data
    require({'Cargo.toml', 'Cargo.lock'} <= sources.keys(), 'baseline Cargo inputs required')
    require(sum(map(len, sources.values())) == 588433, 'expected 588433 baseline bytes')
    cli_source = payload(cli, 'cli')
    require(len(cli_source) == 75601, 'expected 75601 CLI bytes')
    proof = dict(manifest_path='tests/postgres/fixtures/w7/manifest.json',
                 manifest_sha256=hashlib.sha256(raw).hexdigest(), sources=manifest)
    return dict(baseline=sources, cli=cli_source, proof=proof)
