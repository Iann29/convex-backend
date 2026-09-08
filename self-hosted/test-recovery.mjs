// Linux-only local gate for scheduled-mutation recovery after a forced system
// timeout (audit P4 / gate E4). Requires Docker via sudo -n and an already
// built Convex CLI checkout. No package installation, publication or real data.
//
//   CANDIDATE_CPUSET=0 CONVEX_CLI_REPO=/path/to/checkout \
//     node self-hosted/test-recovery.mjs IMAGE [--engine sqlite|postgres]
//       [--persist-seconds 300] [--expect c4|c5] [--out results.json]
//
// Phases per engine, one backend container recreated on the same volume:
//   P0 normal knobs: deploy fixture, schedule four jobs due at T, stop early.
//   P1 DATABASE_UDF_SYSTEM_TIMEOUT_SECONDS=0 + FUNRUN_ISOLATE_ACTIVE_THREADS=1
//      + a V8 "hog" action kept running by the harness. The hog holds the
//      single active-isolate permit while it spins, so every other isolate
//      execution parks in the `concurrency_permit_reacquire` pause at its first
//      syscall. The timeout poller only wakes at the 1 s user deadline; a
//      function still paused at that instant dies with SystemTimeoutError
//      (budget 0). A fast function that finishes before 1 s would survive the
//      knob alone, which is why the knob by itself is not an injection.
//      Observe /metrics and logs for --persist-seconds. Cancel one job through
//      a Node action (a Rust callback, no isolate permit).
//   P2 normal knobs again: retried jobs resolve; assert states and messages.
// Jobs: T1 mutation (transient, must run once after P2 on C5), F mutation
// that throws (must end failed), N Node action writing via runMutation
// (actions are never retried), E mutation canceled during P1.
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { cp, mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { parseArgs, promisify } from "node:util";

const exec = promisify(execFile);
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const cpuset = process.env.CANDIDATE_CPUSET;
assert(cpuset, "Set CANDIDATE_CPUSET to allowed logical CPUs, e.g. 0");
const cliRepo = process.env.CONVEX_CLI_REPO;
assert(cliRepo, "Set CONVEX_CLI_REPO to the checkout containing the built CLI");
const cli = path.join(cliRepo, "npm-packages/convex/dist/cli.bundle.cjs");
const require = createRequire(import.meta.url);
const { ConvexHttpClient } = require(
  path.join(cliRepo, "npm-packages/convex/dist/cjs/browser/index.js"),
);
const { values: opts, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    engine: { type: "string", multiple: true },
    "persist-seconds": { type: "string", default: "300" },
    expect: { type: "string" },
    out: { type: "string" },
    // Record instead of assert when the probe query survives the knob: the
    // system timeout only fires if the timeout poller wakes during a pause,
    // so a fast query can slip through. The scheduled jobs are the real test.
    "probe-soft": { type: "boolean", default: false },
    // Diagnostic: under starvation, time a few HTTP mutations and queries and
    // stop. Tells whether both share the hogs' permit and how each dies.
    "probe-only": { type: "boolean", default: false },
  },
});
const tag = positionals[0];
assert(tag, "Pass a local candidate image tag or ID");
const engines = opts.engine?.length ? opts.engine : ["sqlite", "postgres"];
const persistSeconds = Number(opts["persist-seconds"]);
assert(persistSeconds >= 30, "--persist-seconds must be at least 30");
assert(
  !opts.expect || ["c4", "c5"].includes(opts.expect),
  "--expect must be c4 or c5",
);
// Lab cap on the scheduler's retry backoff (default 2 h). Keeps P2 bounded
// without touching the mechanism under test; reported in the results.
const LAB_MAX_BACKOFF_SECS = "60";
const owner = `amage-recovery-${randomUUID().slice(0, 8)}`;
const containers = [],
  volumes = [],
  secrets = [];
let network, fixture;
const sanitize = (value) =>
  secrets.reduce(
    (s, secret) => s.replaceAll(secret, "[REDACTED]"),
    String(value),
  );
const log = (record) => console.log(JSON.stringify(record));

async function run(command, args, options = {}) {
  try {
    const result = await exec(command, args, {
      timeout: 120000,
      maxBuffer: 32 * 1024 * 1024,
      ...options,
    });
    return result.stdout.trim();
  } catch (error) {
    throw new Error(
      sanitize(
        [error.stderr, error.stdout].filter(Boolean).join("\n") ||
          error.message,
      ),
    );
  }
}
const docker = (args) =>
  run("sudo", [
    "-n",
    "docker",
    "--host",
    "unix:///var/run/docker.sock",
    ...args,
  ]);

async function waitFor(description, check, timeout = 90000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    if (await check()) return;
    await delay(500);
  }
  throw new Error(`Timed out: ${description}`);
}

async function inspect(container) {
  const [info] = JSON.parse(await docker(["inspect", container]));
  return info;
}

async function ready(url, container, version) {
  await waitFor("backend ready", async () => {
    const info = await inspect(container);
    assert(info.State.Running, `Backend exited: ${JSON.stringify(info.State)}`);
    try {
      const response = await fetch(`${url}/version`, {
        signal: AbortSignal.timeout(2000),
      });
      if (!response.ok) return false;
      assert.equal(await response.text(), version);
      return true;
    } catch (error) {
      if (error instanceof assert.AssertionError) throw error;
      return false;
    }
  });
}

// Scheduler counters from the Prometheus endpoint. `scheduled_job_result_total`
// is keyed by status; `success` also counts jobs whose UDF failed (the executor
// saw Ok), which is exactly the trap documented in the audit.
async function metrics(url) {
  const text = await (
    await fetch(`${url}/metrics`, { signal: AbortSignal.timeout(5000) })
  ).text();
  const result = {};
  for (const match of text.matchAll(
    /^\S*scheduled_job_result_total\{([^}]*)\}\s+(\S+)/gm,
  )) {
    const status = /status="([^"]*)"/.exec(match[1])?.[1] ?? "";
    result[status] = (result[status] ?? 0) + Number(match[2]);
  }
  return result;
}
const deltaOf = (before, after) =>
  Object.fromEntries(
    [...new Set([...Object.keys(before), ...Object.keys(after)])].map((k) => [
      k,
      (after[k] ?? 0) - (before[k] ?? 0),
    ]),
  );

async function makeFixture() {
  fixture = await mkdtemp(path.join(tmpdir(), `${owner}-`));
  await cp(
    path.join(root, "npm-packages/demos/system-tables/convex"),
    path.join(fixture, "convex"),
    { recursive: true },
  );
  await mkdir(path.join(fixture, "node_modules"));
  await symlink(
    path.join(cliRepo, "npm-packages/convex"),
    path.join(fixture, "node_modules/convex"),
  );
  await writeFile(
    path.join(fixture, "package.json"),
    JSON.stringify({ private: true, dependencies: { convex: "*" } }),
  );
  await writeFile(
    path.join(fixture, "convex/recovery.ts"),
    `
import { action, mutation } from "./_generated/server";
import { api } from "./_generated/api";
import { v } from "convex/values";
// Real database round trips before acting. A pure insert is buffered in the
// transaction synchronously and never releases the isolate permit; a read is
// an async syscall that releases it, which is where a starved function parks.
// Mirrors real handlers, which read before they write.
async function readFirst(ctx) {
  for (let i = 0; i < 3; i++) await ctx.db.query("messages").first();
}
export const writeAfterRead = mutation({ args: { body: v.string(), author: v.string() }, handler: async (ctx, a) => {
  await readFirst(ctx);
  await ctx.db.insert("messages", { body: a.body, author: a.author, format: "text" }); } });
export const failing = mutation({ args: {}, handler: async (ctx) => {
  await readFirst(ctx);
  throw new Error("AMAGE_EXPECTED_DETERMINISTIC_FAILURE"); } });
export const scheduleWrite = mutation({ args: { at: v.number(), body: v.string() }, handler: (ctx, a) =>
  ctx.scheduler.runAt(a.at, api.recovery.writeAfterRead, { body: a.body, author: "recovery" }) });
export const scheduleFailing = mutation({ args: { at: v.number() }, handler: (ctx, a) =>
  ctx.scheduler.runAt(a.at, api.recovery.failing, {}) });
export const scheduleNodeWrite = mutation({ args: { at: v.number(), body: v.string() }, handler: (ctx, a) =>
  ctx.scheduler.runAt(a.at, api.node.write, { body: a.body }) });
// V8 action (not Node): spins without awaiting, so it holds the active-isolate
// permit for its whole run. Not subject to DATABASE_UDF_SYSTEM_TIMEOUT.
export const hog = action({ args: { iterations: v.number() }, handler: async (_ctx, a) => {
  let x = 0; for (let i = 0; i < a.iterations; i++) x = (x * 31 + i) | 0; return x; } });
`,
  );
  await writeFile(
    path.join(fixture, "convex/node.ts"),
    `
"use node";
import { action } from "./_generated/server";
import { api } from "./_generated/api";
import { v } from "convex/values";
export const write = action({ args: { body: v.string() }, handler: async (ctx, args) => {
  await ctx.runMutation(api.recovery.writeAfterRead, { body: args.body, author: "recovery-node" }); } });
// scheduler.cancel from a Node action is the /api/actions/cancel_job callback:
// pure Rust, no isolate, so it works while DATABASE_UDF_SYSTEM_TIMEOUT is 0.
export const cancel = action({ args: { jobId: v.id("_scheduled_functions") }, handler: async (ctx, args) => {
  await ctx.scheduler.cancel(args.jobId); return "canceled"; } });
`,
  );
}

async function runCase(image, version, pg, engine, ip) {
  const name = `${owner}-${engine}`;
  const volume = await docker([
    "volume",
    "create",
    "--label",
    `amage.smoke=${owner}`,
    name,
  ]);
  volumes.push(volume);
  const url = `http://${ip}:3210`;
  const secret = randomBytes(32).toString("hex");
  secrets.push(secret);
  const env = {
    INSTANCE_NAME: name,
    INSTANCE_SECRET: secret,
    DISABLE_BEACON: "true",
    CONVEX_CLOUD_ORIGIN: url,
    CONVEX_SITE_ORIGIN: `http://${ip}:3211`,
    RUST_LOG: "info",
    SCHEDULED_JOB_MAX_BACKOFF_SECS: LAB_MAX_BACKOFF_SECS,
  };
  if (engine === "postgres") {
    const db = name.replaceAll("-", "_");
    await docker(["exec", pg.id, "createdb", "-U", "postgres", db]);
    env.POSTGRES_URL = `postgresql://postgres:${pg.password}@${pg.ip}:5432`;
    env.DO_NOT_REQUIRE_SSL = "1";
  }
  let container;
  async function startBackend(phase, extraEnv) {
    container = await docker([
      "create",
      "--name",
      name,
      "--label",
      `amage.smoke=${owner}`,
      "--network",
      network,
      "--ip",
      ip,
      "--memory=3g",
      "--cpus=2",
      "--cpuset-cpus",
      cpuset,
      "-v",
      `${volume}:/convex/data`,
      ...Object.entries({ ...env, ...extraEnv }).flatMap(([k, v]) => [
        "-e",
        `${k}=${v}`,
      ]),
      image,
    ]);
    containers.push(container);
    await docker(["start", container]);
    await ready(url, container, version);
    log({ stage: `${phase}-started`, engine, extraEnv });
  }
  async function stopBackend() {
    await docker(["stop", container]);
    const info = await inspect(container);
    assert.equal(info.State.OOMKilled, false);
    const logs = await run("sudo", [
      "-n",
      "docker",
      "--host",
      "unix:///var/run/docker.sock",
      "logs",
      container,
    ]).catch((error) => sanitize(error.message));
    await docker(["rm", container]);
    containers.splice(containers.indexOf(container), 1);
    return { logs, exitCode: info.State.ExitCode };
  }

  // P0: deploy and schedule with normal knobs.
  await startBackend("p0", {});
  const info = await inspect(container);
  const key = await docker(["exec", container, "./generate_admin_key.sh"]);
  secrets.push(key);
  const cliArgs = [
    "-n",
    "nsenter",
    "-t",
    String(info.State.Pid),
    "-n",
    "-S",
    String(process.getuid()),
    "-G",
    String(process.getgid()),
    "--",
    "/usr/bin/env",
    "-i",
    `PATH=${process.env.PATH}`,
    `HOME=${process.env.HOME}`,
    "CI=1",
    `CONVEX_SELF_HOSTED_URL=${url}`,
    `CONVEX_SELF_HOSTED_ADMIN_KEY=${key}`,
    process.execPath,
    cli,
  ];
  await run(
    "sudo",
    [
      ...cliArgs,
      "deploy",
      "-y",
      "--typecheck",
      "disable",
      "--codegen",
      "disable",
      "--url",
      url,
      "--admin-key",
      key,
    ],
    { cwd: fixture },
  );
  const client = new ConvexHttpClient(url);
  client.setAdminAuth(key);
  const bodies = {
    T1: `recovery-transient ${randomUUID()}`,
    N: `recovery-node ${randomUUID()}`,
    E: `recovery-canceled ${randomUUID()}`,
  };
  // Jobs are spaced out on purpose. The isolate client dispatches external
  // requests one at a time in arrival order, so functions that arrive together
  // hand the single permit to each other in milliseconds and only the one whose
  // release lands on a queued hog starves. Alone, each job is followed by a
  // hog in the queue and parks on its first syscall.
  const dueAt = Date.now() + 45000;
  const dueTimes = { T1: dueAt, F: dueAt + 8000, N: dueAt + 16000, E: dueAt + 24000 };
  const jobs = {
    T1: await client.mutation("recovery:scheduleWrite", {
      at: dueTimes.T1,
      body: bodies.T1,
    }),
    F: await client.mutation("recovery:scheduleFailing", { at: dueTimes.F }),
    N: await client.mutation("recovery:scheduleNodeWrite", {
      at: dueTimes.N,
      body: bodies.N,
    }),
    E: await client.mutation("recovery:scheduleWrite", {
      at: dueTimes.E,
      body: bodies.E,
    }),
  };
  let lastRows = [];
  const listJobs = async () => {
    lastRows = await client.query("admin:listScheduledSends", {});
    return Object.fromEntries(
      Object.entries(jobs).map(([k, id]) => [
        k,
        lastRows.find((job) => job._id === id)?.state.kind,
      ]),
    );
  };
  const failureErrors = () =>
    Object.fromEntries(
      Object.entries(jobs).map(([k, id]) => [
        k,
        lastRows.find((job) => job._id === id)?.state.error ?? null,
      ]),
    );
  assert.deepEqual(await listJobs(), {
    T1: "pending",
    F: "pending",
    N: "pending",
    E: "pending",
  });
  const metricsP0 = await metrics(url);
  const p0 = await stopBackend();
  assert(Date.now() < dueAt, "Backend must stop before the jobs become due");
  log({ stage: "p0-scheduled", engine, jobs, dueTimes, metricsP0 });

  // P1: budget 0 plus permit starvation by V8 hog actions.
  await startBackend("p1", {
    DATABASE_UDF_SYSTEM_TIMEOUT_SECONDS: "0",
    FUNRUN_ISOLATE_ACTIVE_THREADS: "1",
    // Without these, the isolate client's CoDel queue expires the initial
    // permit wait (2 s idle, 200 ms congested) and rejects functions before
    // execution with InitialPermitTimeoutError, a different error class that
    // the scheduler already retries and that leaves gaps in the starvation.
    // With them every function waits for its first permit, runs, and parks on
    // `concurrency_permit_reacquire` at its first syscall.
    ISOLATE_QUEUE_IDLE_TIMEOUT_MS: "3600000",
    ISOLATE_QUEUE_CONGESTED_TIMEOUT_MS: "3600000",
  });
  assert(Date.now() < dueAt, "Backend must be back before the jobs become due");
  // Calibrate one hog run on this CPU, then keep two hogs running back to back
  // so the permit is held at every instant of the phase.
  const calibrationStart = Date.now();
  await client.action("recovery:hog", { iterations: 5e7 });
  const calibrationMs = Math.max(1, Date.now() - calibrationStart);
  const hogIterations = Math.min(2e9, Math.max(1e7, Math.round((5e7 * 3000) / calibrationMs)));
  const hog = { runs: 0, errors: [], totalMs: 0, stop: false, iterations: hogIterations, calibrationMs };
  // Stopped on every exit path below; a loop that outlives the run would keep
  // hammering the next run's backend (same bridge IP) with a stale key.
  const hogLoops = [0, 1, 2].map(async () => {
    while (!hog.stop) {
      const start = Date.now();
      try {
        await client.action("recovery:hog", { iterations: hogIterations });
        hog.runs += 1;
        hog.totalMs += Date.now() - start;
      } catch (error) {
        if (hog.errors.length < 5) hog.errors.push(sanitize(error.message).slice(0, 160));
        await delay(100);
      }
    }
  });
  await delay(500);
  const stopHogs = async () => {
    hog.stop = true;
    await Promise.all(hogLoops);
  };
  let injectionError, metricsP1Start, cancelUnderTimeout;
  const samples = [];
  try {
  // The injection must be provably active: any UDF with a syscall dies.
  const probeStart = Date.now();
  try {
    await client.query("messages:list", {});
    injectionError = `probe query returned 200 after ${Date.now() - probeStart} ms`;
    assert(opts["probe-soft"], "queries must fail with the system timeout while the knob is 0");
  } catch (error) {
    if (error instanceof assert.AssertionError) throw error;
    injectionError = error.message;
    assert(
      opts["probe-soft"] || /too many system operations|SystemTimeout/i.test(error.message),
      `unexpected probe error: ${error.message}`,
    );
  }
  metricsP1Start = await metrics(url);
  if (opts["probe-only"]) {
    const probes = [];
    for (let i = 0; i < 5; i++) {
      for (const [kind, call] of [
        ["mutation", () => client.mutation("recovery:writeAfterRead", { body: `probe-${i}`, author: "probe" })],
        ["query", () => client.query("messages:list", {})],
      ]) {
        const start = Date.now();
        try {
          await call();
          probes.push({ kind, ms: Date.now() - start, result: "ok" });
        } catch (error) {
          probes.push({ kind, ms: Date.now() - start, result: sanitize(error.message).split("\n").slice(0, 2).join(" ").slice(0, 120) });
        }
      }
    }
    log({ stage: "probe-only", engine, hog: { runs: hog.runs, meanMs: hog.runs ? hog.totalMs / hog.runs : null }, probes });
    await stopHogs();
    const p1 = await stopBackend();
    if (opts.out) await writeFile(`${opts.out}.probe-${engine}.log`, sanitize(p1.logs));
    return { engine, image, probeOnly: probes };
  }
  // Cancel E after its first attempt has failed, while its retries are
  // failing. The admin endpoint runs in Rust (`scheduling.rs` cancel_job):
  // no isolate, no permit, no Node runtime. (A Node action's
  // `scheduler.cancel` was rejected by the image's node-executor argument
  // validator, which expects a `version` field the client bundle omits.)
  // Falls back to P2 if this path is unavailable on the image.
  await delay(Math.max(0, dueTimes.E + 4000 - Date.now()));
  try {
    const response = await fetch(`${url}/api/cancel_job`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Convex ${key}` },
      body: JSON.stringify({ id: jobs.E }),
      signal: AbortSignal.timeout(10000),
    });
    const text = await response.text();
    assert(response.ok, `cancel_job ${response.status}: ${text.slice(0, 160)}`);
    cancelUnderTimeout = true;
  } catch (error) {
    cancelUnderTimeout = sanitize(error.message);
  }
  const observeUntil = dueTimes.E + persistSeconds * 1000;
  while (Date.now() < observeUntil) {
    await delay(Math.min(10000, Math.max(0, observeUntil - Date.now())));
    const state = (await inspect(container)).State;
    assert(state.Running && !state.OOMKilled, `Backend died: ${JSON.stringify(state)}`);
    const alive = (
      await fetch(`${url}/version`, { signal: AbortSignal.timeout(2000) })
    ).ok;
    assert(alive, "/version must keep answering during the persistent phase");
    samples.push({ t: Date.now() - dueTimes.T1, ...(await metrics(url)) });
  }
  } finally {
    await stopHogs();
  }
  const metricsP1End = await metrics(url);
  const p1 = await stopBackend();
  const retryLines = p1.logs.match(/System error executing job/g) ?? [];
  const retriesPerJob = Object.fromEntries(
    Object.entries(jobs).map(([k, id]) => [
      k,
      (p1.logs.match(new RegExp(`System error executing job ${id}`, "g")) ?? [])
        .length,
    ]),
  );
  // Each scheduler retry line is preceded (same task, a few lines at most)
  // by the reported error. Classify it: an execution timeout is the injection
  // working; a rejection before execution is a different class the scheduler
  // already retried on C4 and must not be credited to the patch.
  const classify = (text) =>
    /too many system operations|maximum total syscall duration|SystemTimeout/i.test(text)
      ? "system_timeout"
      : /Couldn't acquire a permit|rejected_before_execution|InitialPermitTimeout|ExpiredInQueue/i.test(text)
        ? "rejected_before_execution"
        : "other";
  // The scheduler reports the error (a "Caught … error" line) and then logs
  // "System error executing job <id>" from the same task, microseconds apart.
  // Attribute an error to a retry only when it is the single error line in the
  // 50 ms before that retry and no other job's retry sits in between; anything
  // else counts as ambiguous rather than being credited to a job.
  const stripAnsi = (line) => line.replace(/\x1b\[[0-9;]*m/g, "");
  const timeOf = (line) => {
    const match = /^(\d{4}-\d\d-\d\dT[\d:.]+Z)/.exec(line);
    return match ? Date.parse(match[1]) : null;
  };
  const logLines = p1.logs.split("\n").map(stripAnsi);
  const errorLines = [];
  const retryEvents = [];
  logLines.forEach((line, index) => {
    if (/Caught \S+ error/.test(line)) errorLines.push({ index, t: timeOf(line), line });
    for (const [k, id] of Object.entries(jobs))
      if (line.includes(`System error executing job ${id}`))
        retryEvents.push({ index, t: timeOf(line), job: k });
  });
  const attemptsByClass = Object.fromEntries(Object.keys(jobs).map((k) => [k, {}]));
  for (const retry of retryEvents) {
    const window = errorLines.filter(
      (e) => e.index < retry.index && retry.t !== null && e.t !== null && retry.t - e.t <= 50,
    );
    const others = retryEvents.filter(
      (r) => r.job !== retry.job && window.some((e) => e.index < r.index && r.index < retry.index),
    );
    const cls = window.length === 1 && others.length === 0 ? classify(window[0].line) : "ambiguous";
    attemptsByClass[retry.job][cls] = (attemptsByClass[retry.job][cls] ?? 0) + 1;
  }
  if (opts.out) await writeFile(`${opts.out}.p1-${engine}.log`, sanitize(p1.logs));
  const p1Summary = {
    stage: "p1-persistent-timeout",
    engine,
    persistSeconds,
    injectionError,
    hog: { runs: hog.runs, meanMs: hog.runs ? hog.totalMs / hog.runs : null, iterations: hog.iterations, calibrationMs: hog.calibrationMs, errors: hog.errors },
    cancelUnderTimeout,
    resultTotalDelta: deltaOf(metricsP1Start, metricsP1End),
    retryLogLines: retryLines.length,
    retriesPerJob,
    attemptsByClass,
    systemTimeoutLogLines: (p1.logs.match(/SystemTimeout/g) ?? []).length,
    exitCode: p1.exitCode,
    samples,
  };
  log(p1Summary);

  // P2: normal knobs; retried jobs resolve, canceled job must never run.
  await startBackend("p2", {});
  const p2Start = Date.now();
  if (cancelUnderTimeout !== true) {
    // Raced fallback: the retry may already have run; invariants below hold
    // either way and the outcome is reported.
    await client.mutation("admin:cancelMessage", { jobId: jobs.E }).catch(
      (error) => log({ stage: "p2-fallback-cancel-error", error: sanitize(error.message) }),
    );
  }
  const settled = (kind) => kind && kind !== "pending" && kind !== "inProgress";
  let states;
  await waitFor(
    "all jobs settle after the timeout is lifted",
    async () => {
      states = await listJobs();
      return Object.values(states).every(settled);
    },
    (Number(LAB_MAX_BACKOFF_SECS) + 120) * 1000,
  );
  const messages = await client.query("messages:list", {});
  const count = (body) => messages.filter((row) => row.body === body).length;
  const counts = { T1: count(bodies.T1), N: count(bodies.N), E: count(bodies.E) };
  const metricsP2 = await metrics(url);
  const p2 = await stopBackend();
  const errors = failureErrors();
  const p2Summary = {
    stage: "p2-normalized",
    engine,
    states,
    failureErrors: Object.fromEntries(
      Object.entries(errors).map(([k, v]) => [k, v ? String(v).slice(0, 160) : null]),
    ),
    messages: counts,
    settleMs: Date.now() - p2Start,
    resultTotalDeltaSinceP0: deltaOf(metricsP0, metricsP2),
    exitCode: p2.exitCode,
  };
  log(p2Summary);

  // Injection validity, decided before any expectation. T1 and F must show an
  // execution timeout during P1: on a retrying image as a classified retry, on
  // a non-retrying image as the failed-state error. A target that only shows
  // rejections, or none at all, means the starvation had a gap for that job;
  // the cell is then inconclusive and nothing is attributed to the image. E is
  // not required to time out: the cancel is issued while its first attempt is
  // still waiting for the permit, and its proof is `canceled` with no write.
  const timedOut = (k) =>
    (attemptsByClass[k].system_timeout ?? 0) > 0 ||
    classify(errors[k] ?? "") === "system_timeout";
  const injection = Object.fromEntries(["T1", "F", "E"].map((k) => [k, timedOut(k)]));
  const injectionValid = injection.T1 && injection.F;
  log({ stage: "injection-validity", engine, injection, injectionValid, attemptsByClass });
  if (!injectionValid) {
    log({ stage: "inconclusive", engine, reason: "a target job never hit an execution timeout during P1; nothing attributed to the image" });
    process.exitCode = 2;
    return { engine, image, jobs, dueTimes, injectionValid, p1: p1Summary, p2: p2Summary };
  }
  assert(counts.T1 <= 1, "T1 must never run twice");
  assert.equal(counts.N, 0, "N's mutation must never commit: the action failed");
  assert.equal(states.F, "failed", "a deterministic user error must end failed");
  assert.equal(states.N, "failed", "actions are never retried by the scheduler");
  if (states.E === "canceled") assert.equal(counts.E, 0, "canceled job ran");
  else assert(counts.E <= 1, "E must never run twice");
  if (states.T1 === "success") assert.equal(counts.T1, 1, "T1 success without its write");
  if (states.T1 === "failed") assert.equal(counts.T1, 0, "T1 failed but wrote");
  const observed = {
    transientRecovered: states.T1 === "success" && counts.T1 === 1,
    canceledNeverRan: states.E === "canceled" && counts.E === 0,
    cancelUnderTimeout: cancelUnderTimeout === true,
    retryLogLines: retryLines.length,
  };
  if (opts.expect === "c5") {
    assert(observed.transientRecovered, "C5: T1 must run exactly once after normalization");
    assert(retryLines.length >= 3, "C5: persistent phase must show repeated system-error retries");
    assert(observed.canceledNeverRan, "C5: canceled job must never run");
  } else if (opts.expect === "c4") {
    assert.equal(states.T1, "failed", "C4: T1 is terminally failed");
    assert.equal(counts.T1, 0, "C4: T1 never wrote");
    assert.equal(retryLines.length, 0, "C4: no system-error retries");
  }
  log({ stage: "passed", engine, expect: opts.expect ?? "invariants-only", observed });
  return { engine, image, jobs, dueTimes, injectionValid, labMaxBackoffSecs: LAB_MAX_BACKOFF_SECS, p1: p1Summary, p2: p2Summary, observed };
}

const results = { image: tag, expect: opts.expect ?? null, persistSeconds, cases: [] };
try {
  const [image] = JSON.parse(await docker(["image", "inspect", tag]));
  const version = image.Config.Labels["org.opencontainers.image.version"];
  assert(version, "image must carry org.opencontainers.image.version");
  results.imageId = image.Id;
  results.version = version;
  results.labels = Object.fromEntries(
    Object.entries(image.Config.Labels).filter(([k]) => k.startsWith("org.")),
  );
  log({ imageId: image.Id, version, owner, engines, persistSeconds });
  await makeFixture();
  network = await docker([
    "network",
    "create",
    "--internal",
    "--label",
    `amage.smoke=${owner}`,
    owner,
  ]);
  const [net] = JSON.parse(await docker(["network", "inspect", network]));
  assert(net.Internal);
  const prefix = net.IPAM.Config[0].Gateway.split(".").slice(0, 3).join(".");
  let pg;
  if (engines.includes("postgres")) {
    const password = randomBytes(24).toString("hex");
    secrets.push(password);
    const pgIp = `${prefix}.2`;
    const pgId = await docker([
      "create",
      "--name",
      `${owner}-pg`,
      "--label",
      `amage.smoke=${owner}`,
      "--network",
      network,
      "--ip",
      pgIp,
      "--memory=1g",
      "--cpus=1",
      "-e",
      `POSTGRES_PASSWORD=${password}`,
      "postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280",
    ]);
    containers.push(pgId);
    await docker(["start", pgId]);
    await waitFor("Postgres ready", async () => {
      try {
        await docker(["exec", pgId, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"]);
        return true;
      } catch {
        return false;
      }
    });
    pg = { id: pgId, ip: pgIp, password };
  }
  for (const [index, engine] of engines.entries())
    results.cases.push(
      await runCase(image.Id, version, pg, engine, `${prefix}.${index + 3}`),
    );
  console.log("RECOVERY GATE PASSED");
} catch (error) {
  console.error(sanitize(error.stack || error));
  process.exitCode = 1;
  results.error = sanitize(error.message);
  for (const id of containers) {
    const logs = await exec("sudo", [
      "-n",
      "docker",
      "--host",
      "unix:///var/run/docker.sock",
      "logs",
      "--tail",
      "40",
      id,
    ]).catch(() => ({ stdout: "", stderr: "" }));
    console.error(sanitize(logs.stdout + logs.stderr));
  }
} finally {
  for (const id of containers.reverse()) {
    try {
      await docker(["rm", "-f", "-v", id]);
    } catch (error) {
      console.error(sanitize(error.message));
      process.exitCode = 1;
    }
  }
  for (const volume of volumes) {
    try {
      await docker(["volume", "rm", volume]);
    } catch (error) {
      console.error(sanitize(error.message));
      process.exitCode = 1;
    }
  }
  if (network) {
    try {
      await docker(["network", "rm", network]);
    } catch (error) {
      console.error(sanitize(error.message));
      process.exitCode = 1;
    }
  }
  if (fixture) await rm(fixture, { recursive: true, force: true });
  if (opts.out) await writeFile(opts.out, JSON.stringify(results, null, 2));
  console.log(
    "Cleanup completed for this run's containers, volumes, network and fixture.",
  );
  process.exit(process.exitCode ?? 0);
}
