#!/usr/bin/env python3
"""Pair every emitted webhook with its HTTP access log; never subtract percentiles."""
import argparse, json, math, pathlib, re

def stats(values):
    values = sorted(values)
    assert values
    return {name: values[math.ceil(len(values) * q) - 1]
            for name, q in [('p50', .5), ('p95', .95), ('p99', .99), ('max', 1)]}

def analyze(directory):
    root = pathlib.Path(directory)
    run = json.loads((root / 'requests.json').read_text())
    logs = re.sub(r'\x1b\[[0-9;]*m', '', (root / 'backend.log').read_text())
    pattern = r'"POST (/http)?/audit/inbound\?audit_id=(load-\d+) HTTP/1\.1" (\d+) .*? ([\d.]+)ms'
    matches = {}
    for hop, request_id, status, ms in re.findall(pattern, logs):
        key = (request_id, bool(hop))
        assert key not in matches, f'duplicate access log: {key}'
        assert status == '200'
        matches[key] = float(ms)
    direct = not any(inner for _, inner in matches)
    pairs = []
    for request in run['requests']:
        assert 'error' not in request, request
        request_id = request['id']
        handler_ms = matches[(request_id, not direct)]
        site_ms = matches[(request_id, False)]
        pairs.append({**request, 'handlerLogMs': handler_ms, 'siteLogMs': site_ms,
                      'gapMs': request['totalMs'] - handler_ms,
                      'headerGapMs': request['headMs'] - handler_ms,
                      'siteGapMs': request['totalMs'] - site_ms})
    assert len(matches) == len(pairs) * (1 if direct else 2)
    has_socket_wait = all('socketWaitMs' in p for p in pairs)
    if has_socket_wait:
        for p in pairs:
            p['gapAfterSocketMs'] = p['gapMs'] - p['socketWaitMs']
    result = {key: value for key, value in run.items() if key != 'requests'}
    result.update({
        'direct': direct, 'pairedRequests': len(pairs), 'accessLines': len(matches),
        'statsMs': {field: stats([p[field] for p in pairs]) for field in
                    (['totalMs', 'headMs', 'handlerLogMs', 'gapMs', 'headerGapMs', 'siteGapMs', 'dispatchLagMs'] + (['socketWaitMs', 'gapAfterSocketMs'] if has_socket_wait else []))},
        'requestsGapAbove20ms': sum(p['gapMs'] >= 20 for p in pairs),
        'reuse': json.loads((root / 'reuse.json').read_text()),
        'burst': json.loads((root / 'burst.json').read_text()),
    })
    (root / 'paired-requests.json').write_text(json.dumps(pairs, indent=2) + '\n')
    (root / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
    return {key: value for key, value in result.items() if key != 'reuse'}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directories', nargs='+')
    args = parser.parse_args()
    for directory in args.directories:
        print(json.dumps({'directory': directory, **analyze(directory)}))
