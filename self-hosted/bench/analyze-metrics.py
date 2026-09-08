#!/usr/bin/env python3
"""Extract per-cell metric deltas from two /metrics dumps plus the paired
requests of the same cell. Stdlib only. Extracts; it does not judge.

  analyze-metrics.py DIR [--before metrics-before-load.prom]
      [--after metrics-after-load.prom] [--requests paired-requests.json]
      [--histogram NAME ...] [--out FILE] [--self-test]

Histograms are the fork's VMHistogram text format: one `_bucket` line per
NON-EMPTY `vmrange="lo...hi"` range (counts per range, not cumulative), plus
`_sum` and `_count`. Percentiles are reported as the range of the bucket that
contains them, never interpolated. Runtime series (`tokio_*`) are reported as
deltas when cumulative and as before/after values when they are depths or
gauges. Families absent from a dump (e.g. no admission histogram on a C4 image)
are reported as absent, not as failures.
"""
import argparse
import json
import math
import os
import re
import statistics
import sys
import tempfile

PREFIXES = ("convex_local_backend_", "convex_")
DEFAULT_HISTOGRAMS = (
    "http_admission_wait_seconds",
    "http_handle_duration_seconds",
    "sqlite_lock_wait_seconds",
    "sqlite_lock_hold_seconds",
    "sqlite_permit_wait_seconds",
    "database_commit_persistence_write_seconds",
    "database_commit_queue_seconds",
)
# Runtime series that are instantaneous rather than cumulative.
RUNTIME_GAUGES = {
    "tokio_workers_count",
    "tokio_global_queue_depth",
    "tokio_total_local_queue_depth",
    "tokio_task_instrumented_count",
}
QUANTILES = (0.5, 0.95, 0.99)
LABEL_RE = re.compile(r'([A-Za-z_][A-Za-z0-9_]*)="((?:[^"\\]|\\.)*)"')
LINE_RE = re.compile(r"^([A-Za-z_:][A-Za-z0-9_:]*)(\{(.*)\})?\s+(\S+)(?:\s+\d+)?$")


def family(name):
    for prefix in PREFIXES:
        if name.startswith(prefix):
            return name[len(prefix):]
    return name


TYPES = {}  # family -> declared `# TYPE` (gauge, counter, vmhistogram, ...)


def parse_prom(path):
    """{(family, labels-tuple-without-vmrange, vmrange-or-None): value}"""
    series = {}
    with open(path, encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if line.startswith("# TYPE "):
                parts = line.split()
                if len(parts) >= 4:
                    TYPES[family(parts[2])] = parts[3]
                continue
            if not line or line.startswith("#"):
                continue
            match = LINE_RE.match(line)
            if not match:
                continue
            name, _, label_text, value = match.groups()
            labels = dict(LABEL_RE.findall(label_text or ""))
            vmrange = labels.pop("vmrange", None)
            key = (family(name), tuple(sorted(labels.items())), vmrange)
            try:
                series[key] = float(value)
            except ValueError:
                continue
    return series


def parse_range(text):
    lo, _, hi = text.partition("...")
    return float(lo), float(hi)


def histogram_delta(before, after, base):
    """Per label-set delta of a VMHistogram family `base`."""
    out = {}
    label_sets = {
        labels
        for (fam, labels, _) in set(before) | set(after)
        if fam in (base + "_bucket", base + "_sum", base + "_count")
    }
    for labels in sorted(label_sets):
        buckets = {}
        for source, sign in ((before, -1), (after, 1)):
            for (fam, lbl, vmrange), value in source.items():
                if fam == base + "_bucket" and lbl == labels and vmrange:
                    buckets[vmrange] = buckets.get(vmrange, 0.0) + sign * value
        count = after.get((base + "_count", labels, None), 0.0) - before.get(
            (base + "_count", labels, None), 0.0
        )
        total = after.get((base + "_sum", labels, None), 0.0) - before.get(
            (base + "_sum", labels, None), 0.0
        )
        ordered = sorted(
            ((parse_range(r), n) for r, n in buckets.items() if n > 0),
            key=lambda item: item[0][0],
        )
        bucket_total = sum(n for _, n in ordered)
        percentiles = {}
        for q in QUANTILES:
            target = q * bucket_total
            running = 0.0
            chosen = None
            for (lo, hi), n in ordered:
                running += n
                if running >= target and n > 0:
                    chosen = (lo, hi)
                    break
            percentiles[f"p{int(q * 100)}"] = (
                {"bucket_lo": chosen[0], "bucket_hi": chosen[1]} if chosen else None
            )
        entry = {
            "labels": dict(labels),
            "count_delta": count,
            "bucket_count_delta": bucket_total,
            "sum_delta_seconds": total,
            "mean_seconds": (total / count) if count > 0 else None,
            "percentile_buckets_seconds": percentiles,
            "max_bucket_hi_seconds": ordered[-1][0][1] if ordered else None,
        }
        if count != bucket_total:
            entry["note"] = "count delta and bucket delta differ; a dump raced a scrape"
        out[json.dumps(dict(labels), sort_keys=True)] = entry
    return out


def runtime_delta(before, after):
    out = {"cumulative_delta": {}, "before_after": {}}
    names = {
        fam
        for (fam, _, vmrange) in set(before) | set(after)
        if fam.startswith("tokio_") and vmrange is None
    }
    for fam in sorted(names):
        for (f_, labels, _), value_after in after.items():
            if f_ != fam:
                continue
            value_before = before.get((fam, labels, None))
            key = fam if not labels else f"{fam}{json.dumps(dict(labels), sort_keys=True)}"
            declared = TYPES.get(fam)
            is_gauge = (
                declared == "gauge"
                if declared in ("gauge", "counter")
                else (fam in RUNTIME_GAUGES or fam.endswith("_depth"))
            )
            if is_gauge:
                out["before_after"][key] = [value_before, value_after]
            else:
                out["cumulative_delta"][key] = (
                    None if value_before is None else value_after - value_before
                )
    return out


def load_requests(path):
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
    if isinstance(data, dict):
        data = data.get("requests") or data.get("paired") or []
    return [r for r in data if isinstance(r, dict)]


def percentile(values, q):
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(q * len(ordered)) - 1))
    return ordered[index]


def summarize_requests(requests):
    def column(name):
        return [float(r[name]) for r in requests if isinstance(r.get(name), (int, float))]

    summary = {"count": len(requests)}
    for name in ("gapMs", "gapAfterSocketMs", "socketWaitMs", "totalMs", "handlerLogMs"):
        values = column(name)
        summary[name] = (
            {
                "mean": statistics.fmean(values),
                "p50": percentile(values, 0.5),
                "p95": percentile(values, 0.95),
                "p99": percentile(values, 0.99),
                "max": max(values),
                "n": len(values),
            }
            if values
            else None
        )
    return summary


def g1_block(admission, requests_summary):
    """Admission histogram versus the external gap: counts and means only."""
    if not admission:
        return {"status": "absent", "note": "no http_admission_wait_seconds in dumps"}
    site = None
    for entry in admission.values():
        if entry["labels"].get("service") == "backend_http_proxy":
            site = entry
    if site is None:
        return {"status": "absent", "note": "no service=backend_http_proxy series"}
    mean_ms = None if site["mean_seconds"] is None else site["mean_seconds"] * 1000.0
    gap = requests_summary.get("gapMs") or {}
    gap_after = requests_summary.get("gapAfterSocketMs") or {}
    block = {
        "status": "present",
        "site_count_delta": site["count_delta"],
        "requests": requests_summary["count"],
        "site_count_matches_requests": site["count_delta"] == requests_summary["count"],
        "admission_mean_ms": mean_ms,
        "gap_mean_ms": gap.get("mean"),
        "gap_after_socket_mean_ms": gap_after.get("mean"),
        "admission_p95_bucket_ms": None,
    }
    p95 = site["percentile_buckets_seconds"].get("p95")
    if p95:
        block["admission_p95_bucket_ms"] = [p95["bucket_lo"] * 1000.0, p95["bucket_hi"] * 1000.0]
    if mean_ms is not None:
        if gap.get("mean") is not None:
            block["residual_mean_ms_vs_gap"] = gap["mean"] - mean_ms
        if gap_after.get("mean") is not None:
            block["residual_mean_ms_vs_gap_after_socket"] = gap_after["mean"] - mean_ms
    return block


def analyze(directory, before_name, after_name, requests_name, histograms):
    before = parse_prom(os.path.join(directory, before_name))
    after = parse_prom(os.path.join(directory, after_name))
    requests_path = os.path.join(directory, requests_name)
    requests = load_requests(requests_path) if os.path.exists(requests_path) else []
    result = {
        "directory": os.path.abspath(directory),
        "dumps": {"before": before_name, "after": after_name, "series_before": len(before), "series_after": len(after)},
        "requests": summarize_requests(requests) if requests else {"count": 0, "note": "no paired requests"},
        "histograms": {},
        "runtime": runtime_delta(before, after),
    }
    for base in histograms:
        delta = histogram_delta(before, after, base)
        result["histograms"][base] = delta if delta else {"status": "absent"}
    admission = result["histograms"].get("http_admission_wait_seconds")
    result["g1"] = g1_block(
        admission if isinstance(admission, dict) and "status" not in admission else None,
        result["requests"],
    )
    return result


def self_test():
    with tempfile.TemporaryDirectory() as tmp:
        prefix = "convex_local_backend_"
        before = f"""# HELP x
# TYPE {prefix}tokio_task_dropped_count gauge
# TYPE {prefix}tokio_total_steal_count counter
{prefix}tokio_task_dropped_count{{task="axum_handler"}} 9
{prefix}http_admission_wait_seconds_sum{{service="backend_http_proxy"}} 0.1
{prefix}http_admission_wait_seconds_count{{service="backend_http_proxy"}} 2
{prefix}http_admission_wait_seconds_bucket{{service="backend_http_proxy",vmrange="1.000e-3...2.000e-3"}} 2
{prefix}tokio_total_steal_count 100
{prefix}tokio_workers_count 16
{prefix}tokio_global_queue_depth 3
{prefix}sqlite_permit_wait_seconds_count{{flavor="multi",op="write"}} 1
{prefix}sqlite_permit_wait_seconds_sum{{flavor="multi",op="write"}} 0.5
{prefix}sqlite_permit_wait_seconds_bucket{{flavor="multi",op="write",vmrange="4.000e-1...6.000e-1"}} 1
"""
        after = f"""{prefix}tokio_task_dropped_count{{task="axum_handler"}} 4
{prefix}http_admission_wait_seconds_sum{{service="backend_http_proxy"}} 0.6
{prefix}http_admission_wait_seconds_count{{service="backend_http_proxy"}} 12
{prefix}http_admission_wait_seconds_bucket{{service="backend_http_proxy",vmrange="1.000e-3...2.000e-3"}} 7
{prefix}http_admission_wait_seconds_bucket{{service="backend_http_proxy",vmrange="1.000e-2...2.000e-2"}} 3
{prefix}http_admission_wait_seconds_bucket{{service="backend_http_proxy",vmrange="1.000e-1...2.000e-1"}} 2
{prefix}tokio_total_steal_count 160
{prefix}tokio_workers_count 16
{prefix}tokio_global_queue_depth 0
{prefix}sqlite_permit_wait_seconds_count{{flavor="multi",op="write"}} 1
{prefix}sqlite_permit_wait_seconds_sum{{flavor="multi",op="write"}} 0.5
{prefix}sqlite_permit_wait_seconds_bucket{{flavor="multi",op="write",vmrange="4.000e-1...6.000e-1"}} 1
"""
        requests = [
            {"id": f"load-{i}", "gapMs": 60.0, "gapAfterSocketMs": 55.0, "socketWaitMs": 5.0, "totalMs": 500.0}
            for i in range(10)
        ]
        with open(os.path.join(tmp, "metrics-before-load.prom"), "w") as f:
            f.write(before)
        with open(os.path.join(tmp, "metrics-after-load.prom"), "w") as f:
            f.write(after)
        with open(os.path.join(tmp, "paired-requests.json"), "w") as f:
            json.dump(requests, f)
        result = analyze(tmp, "metrics-before-load.prom", "metrics-after-load.prom", "paired-requests.json", DEFAULT_HISTOGRAMS)
        adm = result["histograms"]["http_admission_wait_seconds"][json.dumps({"service": "backend_http_proxy"})]
        assert adm["count_delta"] == 10 and adm["bucket_count_delta"] == 10, adm
        assert abs(adm["mean_seconds"] - 0.05) < 1e-9, adm
        pb = adm["percentile_buckets_seconds"]
        assert pb["p50"] == {"bucket_lo": 0.001, "bucket_hi": 0.002}, pb  # 5 of 10 in first bucket
        assert pb["p95"] == {"bucket_lo": 0.1, "bucket_hi": 0.2}, pb  # 9.5 -> third bucket
        assert pb["p99"] == {"bucket_lo": 0.1, "bucket_hi": 0.2}, pb
        assert adm["max_bucket_hi_seconds"] == 0.2
        g1 = result["g1"]
        assert g1["site_count_matches_requests"] is True, g1
        assert abs(g1["admission_mean_ms"] - 50.0) < 1e-9 and abs(g1["residual_mean_ms_vs_gap"] - 10.0) < 1e-9, g1
        assert abs(g1["residual_mean_ms_vs_gap_after_socket"] - 5.0) < 1e-9, g1
        rt = result["runtime"]
        assert rt["cumulative_delta"]["tokio_total_steal_count"] == 60, rt
        assert rt["before_after"]["tokio_workers_count"] == [16.0, 16.0] and rt["before_after"]["tokio_global_queue_depth"] == [3.0, 0.0], rt
        # Declared `# TYPE gauge` wins over the name heuristic: before/after, never a negative delta.
        assert rt["before_after"]['tokio_task_dropped_count{"task": "axum_handler"}'] == [9.0, 4.0], rt
        assert 'tokio_task_dropped_count{"task": "axum_handler"}' not in rt["cumulative_delta"]
        permit = result["histograms"]["sqlite_permit_wait_seconds"][json.dumps({"flavor": "multi", "op": "write"})]
        assert permit["count_delta"] == 0 and permit["mean_seconds"] is None and permit["percentile_buckets_seconds"]["p95"] is None, permit
        assert result["histograms"]["sqlite_lock_wait_seconds"] == {"status": "absent"}
        assert result["requests"]["gapMs"]["p95"] == 60.0
        # A C4 dump without the admission histogram is absent, not a failure.
        with open(os.path.join(tmp, "metrics-after-load.prom"), "w") as f:
            f.write(f"{prefix}tokio_total_steal_count 170\n")
        with open(os.path.join(tmp, "metrics-before-load.prom"), "w") as f:
            f.write(f"{prefix}tokio_total_steal_count 100\n")
        result = analyze(tmp, "metrics-before-load.prom", "metrics-after-load.prom", "paired-requests.json", DEFAULT_HISTOGRAMS)
        assert result["g1"]["status"] == "absent", result["g1"]
        assert result["runtime"]["cumulative_delta"]["tokio_total_steal_count"] == 70
    print("self-test ok")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("directory", nargs="?")
    parser.add_argument("--before", default="metrics-before-load.prom")
    parser.add_argument("--after", default="metrics-after-load.prom")
    parser.add_argument("--requests", default="paired-requests.json")
    parser.add_argument("--histogram", action="append", default=[], help="extra histogram family (without prefix)")
    parser.add_argument("--out")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if not args.directory:
        parser.error("DIR is required unless --self-test")
    histograms = tuple(DEFAULT_HISTOGRAMS) + tuple(args.histogram)
    result = analyze(args.directory, args.before, args.after, args.requests, histograms)
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(text + "\n")
    print(text)


if __name__ == "__main__":
    main()
