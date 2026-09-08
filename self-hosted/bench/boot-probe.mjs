// Observational boot probe for the disposable heavy100 fixture with search.
import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";

// Runs inside the backend container so Docker exec overhead is outside each
// query measurement. No credentials are needed for this synthetic public UDF.
async function sampleInsideContainer() {
  const { setTimeout: wait } = await import("node:timers/promises");
  const options = JSON.parse(process.argv[1]);
  const samples = [];
  const start = performance.now();
  const metric = async () => {
    const response = await fetch("http://127.0.0.1:3210/metrics");
    if (!response.ok) throw new Error(`metrics HTTP ${response.status}`);
    return response.text();
  };
  const readyMetrics = options.capture ? await metric() : undefined;
  let stableAtMs = null;
  let stableWindowStartMs = null;
  while (performance.now() - start < options.seconds * 1000) {
    const at = performance.now();
    let ok = false, error;
    try {
      const response = await fetch("http://127.0.0.1:3210/api/query", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ path: "audit:bootRead", args: {}, format: "json" }),
        signal: AbortSignal.timeout(2000),
      });
      const data = await response.json();
      ok = response.ok && data.status === "success" &&
        data.value.tenants === 100 && data.value.searchHits === 1;
      if (!ok) error = JSON.stringify(data);
    } catch (e) {
      error = String(e);
    }
    samples.push({ atMs: at - start, ms: performance.now() - at, ok, error });
    const recent = samples.filter((s) => s.atMs >= at - start - 60000);
    if (options.thresholdMs && stableAtMs === null && at - start >= 60000 &&
        recent.length >= 100 && recent.every((s) => s.ok)) {
      const sorted = recent.map((s) => s.ms).sort((a, b) => a - b);
      if (sorted[Math.ceil(sorted.length * 0.95) - 1] <= options.thresholdMs) {
        stableWindowStartMs = at - start - 60000;
        stableAtMs = performance.now() - start;
      }
    }
    await wait(Math.max(0, at + 500 - performance.now()));
  }
  const afterMetrics = options.capture ? await metric() : undefined;
  process.stdout.write(JSON.stringify({ samples, stableAtMs, stableWindowStartMs, readyMetrics, afterMetrics }));
}

export async function runBootProbe({ docker, backend, version, out }) {
  const save = (name, value) => writeFile(path.join(out, name),
    typeof value === "string" ? value : JSON.stringify(value, null, 2));
  const sample = async (options) => JSON.parse(await docker([
    "exec", backend, "node", "-e", `(${sampleInsideContainer.toString()})()`,
    JSON.stringify(options),
  ], { timeout: (options.seconds + 30) * 1000 }));
  const baseline = await sample({ seconds: 60 });
  await save("boot-baseline.json", baseline);
  assert(baseline.samples.length >= 100 && baseline.samples.every((s) => s.ok),
    "boot fixture must have a populated, queryable search index before restarts");
  const sorted = baseline.samples.map((s) => s.ms).sort((a, b) => a - b);
  const baselineP95Ms = sorted[Math.ceil(sorted.length * 0.95) - 1];
  const rows = [];
  for (let restart = 1; restart <= 3; restart++) {
    await docker(["stop", "--time", "10", backend]);
    const [stopped] = JSON.parse(await docker(["inspect", backend]));
    const startAt = new Date().toISOString(), start = performance.now();
    await docker(["start", backend]);
    let ready = false;
    while (performance.now() - start < 120000) {
      try {
        ready = (await docker(["exec", backend, "curl", "-fsS", "--max-time", "2",
          "http://127.0.0.1:3210/version"])) === version;
        if (ready) break;
      } catch {}
      await delay(200);
    }
    assert(ready, "backend did not become ready after restart");
    const readyMs = performance.now() - start;
    const sampleStartMs = performance.now() - start;
    const data = await sample({ seconds: 120, capture: true, thresholdMs: baselineP95Ms * 1.2 });
    await save(`boot-${restart}-ready.prom`, data.readyMetrics);
    await save(`boot-${restart}-120s.prom`, data.afterMetrics);
    const { readyMetrics, afterMetrics, ...observations } = data;
    await save(`boot-${restart}-queries.json`, observations);
    rows.push({ restart, startAt, stopExitCode: stopped.State.ExitCode,
      stopOOMKilled: stopped.State.OOMKilled, readyMs, baselineP95Ms,
      thresholdMs: baselineP95Ms * 1.2,
      // The 60 s confirmation window is not 60 s of startup latency.
      stableWindowStartMs: data.stableWindowStartMs === null ? null : sampleStartMs + data.stableWindowStartMs,
      stableConfirmedMs: data.stableAtMs === null ? null : sampleStartMs + data.stableAtMs,
      stableCensoredAtMs: data.stableAtMs === null ? performance.now() - start : null });
    await save("boot-restarts.json", rows);
  }
}
