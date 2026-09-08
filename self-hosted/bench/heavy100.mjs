import { countFixture } from "./count-fixture.mjs";
// Local-only HTTP contract/connection/gap probe; no published ports or external routes.
import assert from "node:assert/strict";
import http from "node:http";
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, writeFile, readFile, symlink } from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";

const arg = (name, fallback) => {
  const i = process.argv.indexOf(name);
  return i < 0 ? fallback : process.argv[i + 1];
};
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const out = path.resolve(arg("--out", ""));
assert(arg("--out"), "--out must name a new measurement directory");
const engine = arg("--engine", "sqlite");
assert(["sqlite", "postgres"].includes(engine));
const seconds = Number(arg("--seconds", "120"));
assert(seconds >= 1 && seconds <= 600);
const messagesPerMinute = Number(arg("--rate", "240"));
assert(messagesPerMinute >= 1 && messagesPerMinute <= 10000);
const seedDocuments = Number(arg("--seed-docs", "3000"));
assert(seedDocuments >= 3000 && seedDocuments <= 330000);
const maxSockets = Number(arg("--max-sockets", "64"));
assert(maxSockets >= 16 && maxSockets <= 512);
const requestsPerSecond = messagesPerMinute / 30;
const backendCpus = Number(arg("--backend-cpus", "6"));
assert([2, 6].includes(backendCpus));
const postgresCpus = Number(arg("--postgres-cpus", "2"));
assert([1, 2, 4].includes(postgresCpus));
const pgProfile = arg("--pg-profile", "vanilla");
assert(
  ["vanilla", "wal8", "wal8-buffers1g", "wal8-buffers1g-lz4"].includes(
    pgProfile,
  ),
);
const pgOptions =
  pgProfile === "vanilla"
    ? []
    : [
        "-c",
        "max_wal_size=8GB",
        ...(pgProfile.includes("buffers1g")
          ? ["-c", "shared_buffers=1GB"]
          : []),
        ...(pgProfile.endsWith("-lz4") ? ["-c", "wal_compression=lz4"] : []),
      ];
const siteSlots = Number(arg("--site-slots", "16"));
assert([16, 64].includes(siteSlots));
const sqliteJournal = arg("--sqlite-journal", "delete");
assert(["delete", "wal"].includes(sqliteJournal));
const writeLimitMiB = Number(arg("--write-mib", "4"));
assert([4, 16].includes(writeLimitMiB));
const handlerDelay = Number(arg("--delay-ms", "500"));
assert(handlerDelay >= 0 && handlerDelay <= 2000);
// Select logical CPUs supported by this disposable host.
const cpuset = process.env.CANDIDATE_CPUSET;
assert(cpuset, "Set CANDIDATE_CPUSET to allowed logical CPUs");
const cliRepo = process.env.CONVEX_CLI_REPO;
assert(cliRepo, "Set CONVEX_CLI_REPO to a checkout with the built CLI");
const cli = path.join(cliRepo, "npm-packages/convex/dist/cli.bundle.cjs");
const exec = promisify(execFile),
  secrets = [];
const redact = (x) =>
  secrets.reduce(
    (s, k) => s.replaceAll(k, "[synthetic credential redacted]"),
    String(x),
  );
async function run(cmd, args, options = {}) {
  try {
    return (
      await exec(cmd, args, {
        timeout: 120000,
        maxBuffer: 128 << 20,
        ...options,
      })
    ).stdout.trim();
  } catch (e) {
    throw new Error(
      redact([e.stderr, e.stdout, e.message].filter(Boolean).join("\n")),
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
const sha = (x) => createHash("sha256").update(x).digest("hex");
const quantile = (xs, p) => {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.ceil(p * s.length) - 1];
};

if (process.argv.includes("--driver")) {
  const require = createRequire(import.meta.url);
  const { ConvexHttpClient } = require(
    path.join(cliRepo, "npm-packages/convex/dist/cjs/browser/index.js"),
  );
  const convex = new ConvexHttpClient("http://127.0.0.1:3210");
  convex.setAdminAuth(process.env.AUDIT_ADMIN_KEY);
  async function captureSqlite(name) {
    if (engine !== "sqlite") return;
    const value = await docker([
      "exec",
      process.env.AUDIT_BACKEND_CONTAINER_ID,
      "node",
      "-e",
      "const{DatabaseSync}=require('node:sqlite');const db=new DatabaseSync('/convex/data/db.sqlite3',{readOnly:true});db.exec('PRAGMA busy_timeout=5000');console.log(JSON.stringify({documents:db.prepare('SELECT count(*) AS n FROM documents').get().n,indexes:db.prepare('SELECT count(*) AS n FROM indexes').get().n,sqliteReader:db.prepare('SELECT sqlite_version() AS v').get().v}));db.close()",
    ]);
    await writeFile(
      path.join(out, name + ".json"),
      JSON.stringify(JSON.parse(value), null, 2),
    );
  }
  async function capturePg(name) {
    if (!process.env.AUDIT_PG_CONTAINER_ID) return;
    const value = await docker([
      "exec",
      process.env.AUDIT_PG_CONTAINER_ID,
      "psql",
      "-U",
      "postgres",
      "-d",
      process.env.AUDIT_DATABASE,
      "-At",
      "-c",
      "SELECT row_to_json(s) FROM (SELECT datname,xact_commit,xact_rollback,tup_inserted,tup_updated,tup_deleted,blks_read,blks_hit,(SELECT row_to_json(w) FROM pg_stat_wal w) AS wal,(SELECT row_to_json(c) FROM pg_stat_checkpointer c) AS checkpointer FROM pg_stat_database WHERE datname=current_database()) s",
    ]);
    await writeFile(
      path.join(out, name + ".json"),
      JSON.stringify(JSON.parse(value), null, 2),
    );
  }
  async function captureMetrics(name) {
    const response = await fetch("http://127.0.0.1:3210/metrics");
    assert.equal(response.status, 200);
    await writeFile(path.join(out, name + ".prom"), await response.text());
  }
  await captureMetrics("metrics-before-seed");
  const seedStartAt = new Date().toISOString(),
    seedStart = performance.now(),
    seedProbes = [];
  let seeding = true,
    seedRetries = 0;
  const probe = (async () => {
    while (seeding) {
      const at = new Date().toISOString(),
        start = performance.now();
      try {
        const response = await fetch(
          "http://127.0.0.1:3211/audit/echo/bulk-probe",
          { signal: AbortSignal.timeout(5000) },
        );
        await response.arrayBuffer();
        seedProbes.push({
          at,
          status: response.status,
          ms: performance.now() - start,
        });
      } catch (error) {
        seedProbes.push({
          at,
          error: String(error),
          ms: performance.now() - start,
        });
      }
      if (seeding) await delay(200);
    }
  })();
  try {
    for (let base = 0; base < seedDocuments; base += 500) {
      for (let attempt = 0; ; attempt++) {
        try {
          await convex.mutation("audit:seed", {
            base,
            n: Math.min(500, seedDocuments - base),
          });
          break;
        } catch (e) {
          let meta;
          try {
            meta = JSON.parse(e.message);
          } catch {}
          if (
            meta?.code !== "TooManyWrites" ||
            !meta.message.includes("per second") ||
            attempt >= 5
          )
            throw e;
          seedRetries++;
          await delay(1000);
        }
      }
      if (base % 50000 === 0)
        await writeFile(
          path.join(out, "seed-progress.json"),
          JSON.stringify({
            seeded: base + 500,
            target: seedDocuments,
            elapsedMs: performance.now() - seedStart,
            seedRetries,
          }),
        );
    }
  } finally {
    seeding = false;
    await probe;
    await writeFile(
      path.join(out, "seed-probes.json"),
      JSON.stringify(seedProbes, null, 2),
    );
  }
  await writeFile(
    path.join(out, "seed.json"),
    JSON.stringify({
      documents: seedDocuments,
      elapsedMs: performance.now() - seedStart,
      seedStartAt,
      seedEndAt: new Date().toISOString(),
      seedRetries,
      writeLimitMiB,
      throttleMs: 0,
    }),
  );

  await captureMetrics("metrics-after-seed");
  const agent = new http.Agent({
    keepAlive: true,
    maxSockets,
    maxFreeSockets: maxSockets,
  });
  let connects = 0;
  function request(
    route,
    {
      method = "GET",
      headers = {},
      body,
      chunked = false,
      abortFirst = false,
      port = 3211,
    } = {},
  ) {
    return new Promise((resolve, reject) => {
      const start = performance.now();
      let socketWaitMs;
      const req = http.request(
        {
          hostname: "127.0.0.1",
          port,
          path: route,
          method,
          agent,
          headers: {
            ...headers,
            ...(body && !chunked ? { "content-length": body.length } : {}),
          },
        },
        (res) => {
          const headMs = performance.now() - start,
            chunks = [],
            arrivals = [];
          res.on("data", (chunk) => {
            chunks.push(chunk);
            arrivals.push(performance.now() - start);
            if (abortFirst) res.destroy();
          });
          const finish = () =>
            resolve({
              status: res.statusCode,
              headers: res.headers,
              rawHeaders: res.rawHeaders,
              body: Buffer.concat(chunks),
              socketWaitMs,
              headMs,
              totalMs: performance.now() - start,
              arrivals,
            });
          res.on("end", finish);
          res.on("error", (e) => (abortFirst ? finish() : reject(e)));
          if (abortFirst) res.on("close", finish);
        },
      );
      req.on("socket", (socket) => {
        socketWaitMs = performance.now() - start;
        if (socket.connecting) socket.once("connect", () => connects++);
      });
      req.on("error", reject);
      req.setTimeout(30000, () => req.destroy(new Error("client timeout")));
      if (chunked && body) {
        req.write(body.subarray(0, Math.floor(body.length / 2)));
        setTimeout(
          () => req.end(body.subarray(Math.floor(body.length / 2))),
          5,
        );
      } else req.end(body);
    });
  }
  async function tcp() {
    const text = await readFile("/proc/net/snmp", "utf8");
    const lines = text.split("\n").filter((x) => x.startsWith("Tcp:"));
    const keys = lines[0].split(/\s+/),
      values = lines[1].split(/\s+/);
    const sockets = (await readFile("/proc/net/tcp", "utf8"))
      .split("\n")
      .filter((x) => /:0C8A\b/.test(x));
    return {
      activeOpens: Number(values[keys.indexOf("ActiveOpens")]),
      clientConnects: connects,
      sockets3210: sockets,
    };
  }
  const contracts = [];
  try {
    for (const method of [
      "GET",
      "POST",
      "PUT",
      "PATCH",
      "DELETE",
      "OPTIONS",
      "HEAD",
    ]) {
      const body = ["GET", "HEAD", "OPTIONS"].includes(method)
        ? undefined
        : Buffer.from("signed-body-\u0000-ação");
      const r = await request("/audit/echo/a%2Fb?x=one%2Ftwo&x=second", {
        method,
        body,
        headers: {
          "x-forwarded-proto": "https",
          "x-forwarded-host": "fixture.example",
          "x-audit-header": "preserved",
          authorization: "Bearer synthetic-not-convex",
        },
      });
      assert.equal(r.status, 200, method);
      assert.equal(r.headers["x-audit-response"], "preserved");
      assert.deepEqual(r.headers["set-cookie"], ["a=1; Path=/", "b=2; Path=/"]);
      const data = method === "HEAD" ? null : JSON.parse(r.body);
      if (data) {
        assert.equal(data.method, method);
        assert.equal(data.header, "preserved");
        assert.equal(data.bodySha, sha(body ?? Buffer.alloc(0)));
        assert.equal(
          data.url,
          "https://fixture.example/audit/echo/a%2Fb?x=one%2Ftwo&x=second",
        );
      } else assert.equal(r.body.length, 0);
      contracts.push({
        case: method,
        status: r.status,
        data,
        setCookie: r.headers["set-cookie"],
      });
    }
    const blob = Buffer.alloc(256 << 10);
    for (let i = 0; i < blob.length; i++) blob[i] = i % 251;
    const streamedRequest = await request("/audit/echo/chunked", {
      method: "POST",
      body: blob,
      chunked: true,
    });
    assert.equal(JSON.parse(streamedRequest.body).bodySha, sha(blob));
    contracts.push({
      case: "chunked-request",
      bytes: blob.length,
      sha256: sha(blob),
    });
    for (const [route, method, status] of [
      ["/api/query", "POST", 404],
      ["/audit/missing", "GET", 404],
      ["/audit/echo/path", "TRACE", 405],
    ]) {
      const r = await request(route, { method });
      assert.equal(r.status, status);
      contracts.push({ case: route + ":" + method, status: r.status });
    }
    // RequestId::from_str currently accepts any string; preserve that contract.
    const customId = await request("/audit/echo/path", {
      headers: { "convex-request-id": "arbitrary-fixture-id" },
    });
    assert.equal(customId.status, 200);
    assert.equal(JSON.parse(customId.body).requestId, "arbitrary-fixture-id");
    contracts.push({ case: "custom-request-id", status: customId.status });
    const stream = await request("/audit/stream");
    assert.equal(stream.body.toString(), "first-second-third");
    assert(stream.totalMs - stream.headMs >= 60);
    assert(stream.arrivals.length >= 2);
    contracts.push({
      case: "streamed-response",
      headMs: stream.headMs,
      totalMs: stream.totalMs,
      arrivals: stream.arrivals,
    });
    await request("/audit/stream", { abortFirst: true });
    const recovered = await request("/audit/echo/after-abort");
    assert.equal(recovered.status, 200);
    contracts.push({ case: "client-abort-recovery", status: recovered.status });
    const meta = [];
    for (const port of [3210, 3211])
      for (const route of ["/version", "/metrics"]) {
        const r = await request(route, { port });
        assert.equal(r.status, 200);
        meta.push({
          port,
          route,
          status: r.status,
          value: route === "/version" ? r.body.toString() : undefined,
        });
      }
    await writeFile(
      path.join(out, "contracts.json"),
      JSON.stringify({ contracts, meta }, null, 2),
    );
    // Warm both pools, then count TCP opens while the caller reuses one connection.
    await request("/audit/echo/reuse");
    const beforeReuse = await tcp();
    for (let i = 0; i < 96; i++)
      assert.equal((await request("/audit/echo/reuse")).status, 200);
    const afterReuse = await tcp();
    const reuse = {
      before: beforeReuse,
      after: afterReuse,
      newTcpOpens: afterReuse.activeOpens - beforeReuse.activeOpens,
      newCallerConnections:
        afterReuse.clientConnects - beforeReuse.clientConnects,
    };
    await writeFile(
      path.join(out, "reuse.json"),
      JSON.stringify(reuse, null, 2),
    );
    const burst = await Promise.all(
      Array.from({ length: 128 }, (_, i) =>
        request("/audit/wait?audit_id=burst-" + i + "&delay=250"),
      ),
    );
    const intervals = burst
      .map((x) => JSON.parse(x.body))
      .flatMap((x) => [
        { t: x.start, n: 1 },
        { t: x.end, n: -1 },
      ])
      .sort((a, b) => a.t - b.t || a.n - b.n);
    let active = 0,
      peak = 0;
    for (const event of intervals) {
      active += event.n;
      peak = Math.max(peak, active);
    }
    await writeFile(
      path.join(out, "burst.json"),
      JSON.stringify(
        {
          requests: 128,
          peak,
          p50: quantile(
            burst.map((x) => x.totalMs),
            0.5,
          ),
          p95: quantile(
            burst.map((x) => x.totalMs),
            0.95,
          ),
        },
        null,
        2,
      ),
    );
    assert.equal(peak, siteSlots, "site permit count");
    await writeFile(
      path.join(out, "tcp-after-burst.json"),
      JSON.stringify(await tcp(), null, 2),
    );
    console.log(
      JSON.stringify({
        stage: "contracts-and-reuse",
        engine,
        newTcpOpens: reuse.newTcpOpens,
        newCallerConnections: reuse.newCallerConnections,
        burstPeak: peak,
      }),
    );
    await capturePg("pg-before-load");
    await captureSqlite("sqlite-before-load");
    const loadStartAt = new Date().toISOString();
    const start = performance.now(),
      promises = [],
      count = Math.floor(seconds * requestsPerSecond);
    let completed = 0,
      peakOutstanding = 0;
    const windows = [];
    for (let i = 0; i < count; i++) {
      const scheduled = start + (i * 1000) / requestsPerSecond;
      await delay(Math.max(0, scheduled - performance.now()));
      const dispatched = performance.now();
      const payload = Buffer.from(
        JSON.stringify({
          seq: i,
          company: String(Math.floor(i / 2) % 100),
          kind: i % 2 ? "message.any" : "message",
          delay: handlerDelay,
        }),
      );
      promises.push(
        request("/audit/inbound?audit_id=load-" + i, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: payload,
        })
          .then((r) => {
            assert.equal(r.status, 200, r.body.toString());
            assert.equal(JSON.parse(r.body).seq, i);
            return {
              id: "load-" + i,
              totalMs: r.totalMs,
              headMs: r.headMs,
              socketWaitMs: r.socketWaitMs,
              completedMs: performance.now() - start,
              dispatchLagMs: dispatched - scheduled,
              status: r.status,
            };
          })
          .catch((e) => ({
            id: "load-" + i,
            error: String(e),
            completedMs: performance.now() - start,
          }))
          .finally(() => completed++),
      );
      peakOutstanding = Math.max(peakOutstanding, i + 1 - completed);
      if (i % Math.max(1, Math.round(requestsPerSecond * 15)) === 0) {
        const progress = {
          stage: "load",
          engine,
          dispatched: i,
          total: count,
          completed,
          outstanding: i + 1 - completed,
        };
        windows.push({ ...progress, elapsedMs: performance.now() - start });
        console.log(JSON.stringify(progress));
        await writeFile(
          path.join(out, "progress.json"),
          JSON.stringify(progress),
        );
      }
    }
    const emissionStopMs = performance.now() - start,
      pendingAtStop = count - completed;
    const requests = await Promise.all(promises);
    const drainMs = performance.now() - start - emissionStopMs;
    const loadEndAt = new Date().toISOString();
    const counts = await countFixture(convex);
    const errors = requests.filter((x) => x.error);
    const recovery = await request("/audit/echo/recovery");
    assert.equal(recovery.status, 200);
    await writeFile(
      path.join(out, "stress.json"),
      JSON.stringify(
        {
          loadStartAt,
          loadEndAt,
          emissionStopMs,
          pendingAtStop,
          peakOutstanding,
          drainMs,
          windows,
          errors: errors.length,
          recoveryMs: recovery.totalMs,
          maxSockets,
          seedDocuments,
        },
        null,
        2,
      ),
    );
    await writeFile(
      path.join(out, "tcp-after-load.json"),
      JSON.stringify(await tcp(), null, 2),
    );
    await writeFile(
      path.join(out, "requests.json"),
      JSON.stringify(
        {
          engine,
          seconds,
          requestsPerSecond,
          messagesPerMinute,
          handlerDelayMs: handlerDelay,
          seedDocuments,
          counts,
          requests,
        },
        null,
        2,
      ),
    );
    await captureMetrics("metrics-after-load");
    await delay(1500);
    await capturePg("pg-after-load");
    await captureSqlite("sqlite-after-load");
    assert.equal(errors.length, 0, JSON.stringify(errors.slice(0, 5)));
    assert.equal(counts.count, count);
    assert.equal(counts.unique, count);
    console.log(
      JSON.stringify({
        stage: "load-finished",
        engine,
        count,
        p50: quantile(
          requests.map((x) => x.totalMs),
          0.5,
        ),
        p95: quantile(
          requests.map((x) => x.totalMs),
          0.95,
        ),
        dispatchP95: quantile(
          requests.map((x) => x.dispatchLagMs),
          0.95,
        ),
      }),
    );
  } finally {
    agent.destroy();
  }
} else {
  await mkdir(out, { recursive: false });
  const image = arg("--image");
  assert(image);
  const [img] = JSON.parse(await docker(["image", "inspect", image]));
  const owner = "amage-c4-" + randomUUID().slice(0, 8),
    name = owner + "-" + engine,
    ids = [];
  const secret = randomBytes(32).toString("hex");
  secrets.push(secret);
  let backend,
    pg,
    sampling = false,
    sampler;
  const resourceSamples = [];
  try {
    if (engine === "postgres") {
      pg = await docker([
        "create",
        "--name",
        owner + "-pg",
        "--label",
        "amage.candidate4=" + owner,
        "--network",
        "none",
        "--memory",
        "4g",
        "--memory-swap",
        "4g",
        "--cpus",
        String(postgresCpus),
        "-e",
        "POSTGRES_PASSWORD=" + secret,
        "-e",
        "POSTGRES_DB=" + name.replaceAll("-", "_"),
        "postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280",
        ...pgOptions,
      ]);
      ids.push(pg);
      await docker(["start", pg]);
      let ready = false;
      for (let i = 0; i < 100; i++) {
        try {
          await docker([
            "exec",
            pg,
            "pg_isready",
            "-h",
            "127.0.0.1",
            "-U",
            "postgres",
          ]);
          ready = true;
          break;
        } catch {}
        await delay(250);
      }
      assert(ready);
      const settings = await docker([
        "exec",
        pg,
        "psql",
        "-U",
        "postgres",
        "-d",
        name.replaceAll("-", "_"),
        "-At",
        "-c",
        "SELECT json_agg(s) FROM (SELECT name,setting,unit,source FROM pg_settings WHERE name IN ('max_wal_size','min_wal_size','checkpoint_timeout','checkpoint_completion_target','shared_buffers','work_mem','synchronous_commit','fsync','full_page_writes','wal_compression','max_connections','wal_buffers') ORDER BY name) s",
      ]);
      await writeFile(
        path.join(out, "pg-settings.json"),
        JSON.stringify(
          {
            profile: pgProfile,
            options: pgOptions,
            settings: JSON.parse(settings),
          },
          null,
          2,
        ),
      );
    }
    const env = {
      INSTANCE_NAME: name,
      INSTANCE_SECRET: secret,
      MAX_BYTES_WRITTEN_PER_SECOND: String(writeLimitMiB * 2 ** 20),
      SITE_MAX_CONCURRENT_REQUESTS: String(siteSlots),
      DISABLE_BEACON: "true",
      DISABLE_METRICS_ENDPOINT: "false",
      CONVEX_CLOUD_ORIGIN: "http://127.0.0.1:3210",
      CONVEX_SITE_ORIGIN: "http://127.0.0.1:3211",
      RUST_LOG: "info",
    };
    if (pg) {
      env.POSTGRES_URL =
        "postgresql://postgres:" + secret + "@127.0.0.1:5432?sslmode=disable";
      env.DO_NOT_REQUIRE_SSL = "1";
    }
    backend = await docker([
      "create",
      "--name",
      name,
      "--label",
      "amage.candidate4=" + owner,
      "--network",
      pg ? "container:" + pg : "none",
      "--memory",
      "8g",
      "--memory-swap",
      "8g",
      "--cpus",
      String(backendCpus),
      "--cpuset-cpus",
      cpuset,
      ...Object.entries(env).flatMap(([k, v]) => ["-e", k + "=" + v]),
      img.Id,
    ]);
    ids.push(backend);
    await docker(["start", backend]);
    const version = img.Config.Labels["org.opencontainers.image.version"];
    let ready = false;
    for (let i = 0; i < 120; i++) {
      try {
        if (
          (await docker([
            "exec",
            backend,
            "curl",
            "-fsS",
            "--max-time",
            "2",
            "http://127.0.0.1:3210/version",
          ])) === version
        ) {
          ready = true;
          break;
        }
      } catch {}
      await delay(500);
    }
    assert(ready);
    // Configure the existing disposable DB offline; SQLite retains WAL in its header.
    if (engine === "sqlite") {
      await docker(["stop", "--time", "30", backend]);
      const init = await docker([
        "run",
        "--rm",
        "--network",
        "none",
        "--volumes-from",
        backend,
        "--entrypoint",
        "node",
        img.Id,
        "-e",
        "const{DatabaseSync}=require('node:sqlite');const db=new DatabaseSync('/convex/data/db.sqlite3');const mode=db.prepare('PRAGMA journal_mode=" +
          sqliteJournal +
          "').get();console.log(JSON.stringify({mode,synchronous:db.prepare('PRAGMA synchronous').get(),pageSize:db.prepare('PRAGMA page_size').get(),sqliteReader:db.prepare('SELECT sqlite_version() AS v').get()}));db.close()",
      ]);
      const setup = JSON.parse(init);
      assert.equal(Object.values(setup.mode)[0], sqliteJournal);
      assert.equal(Object.values(setup.synchronous)[0], 2);
      await writeFile(
        path.join(out, "sqlite-journal-setup.json"),
        JSON.stringify(setup, null, 2),
      );
      await docker(["start", backend]);
      let up = false;
      for (let i = 0; i < 120; i++) {
        try {
          if (
            (await docker([
              "exec",
              backend,
              "curl",
              "-fsS",
              "--max-time",
              "2",
              "http://127.0.0.1:3210/version",
            ])) === version
          ) {
            up = true;
            break;
          }
        } catch {}
        await delay(500);
      }
      assert(up, "backend after journal setup");
    }
    const [info] = JSON.parse(await docker(["inspect", backend]));
    assert.deepEqual(info.HostConfig.PortBindings, {});
    assert.equal(
      (await docker(["exec", backend, "cat", "/proc/net/route"])).split("\n")
        .length,
      1,
    );
    await writeFile(
      path.join(out, "identity.json"),
      JSON.stringify(
        {
          engine,
          owner,
          id: backend,
          image: img.Id,
          version,
          labels: img.Config.Labels,
          memory: info.HostConfig.Memory,
          nanoCpus: info.HostConfig.NanoCpus,
          cpuset: info.HostConfig.CpusetCpus,
          writeLimitMiB,
          companies: 100,
          storage: "Docker anonymous volumes on instance disk",
          mounts: info.Mounts,
          network: info.HostConfig.NetworkMode,
          ports: info.HostConfig.PortBindings,
        },
        null,
        2,
      ),
    );
    const key = await docker(["exec", backend, "./generate_admin_key.sh"]);
    secrets.push(key);
    const fixture = path.join(out, "fixture");
    await mkdir(path.join(fixture, "convex"), { recursive: true });
    await mkdir(path.join(fixture, "node_modules"));
    await symlink(
      path.join(cliRepo, "npm-packages/convex"),
      path.join(fixture, "node_modules/convex"),
    );
    await writeFile(
      path.join(fixture, "package.json"),
      JSON.stringify({ private: true, dependencies: { convex: "*" } }),
    );
    for (const name of ["schema.ts", "audit.ts", "http.ts"])
      await writeFile(
        path.join(fixture, "convex", name),
        await readFile(path.join(root, "bench/fixture-soak100", name)),
      );
    const enter = [
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
      "PATH=" + process.env.PATH,
      "CI=1",
      "CANDIDATE_CPUSET=" + cpuset,
      "CONVEX_CLI_REPO=" + cliRepo,
    ];
    await writeFile(
      path.join(out, "deploy.log"),
      await run(
        "sudo",
        [
          ...enter,
          "CONVEX_SELF_HOSTED_URL=http://127.0.0.1:3210",
          "CONVEX_SELF_HOSTED_ADMIN_KEY=" + key,
          process.execPath,
          cli,
          "deploy",
          "-y",
          "--typecheck",
          "disable",
          "--codegen",
          "disable",
          "--url",
          "http://127.0.0.1:3210",
          "--admin-key",
          key,
        ],
        { cwd: fixture },
      ),
    );
    if (process.argv.includes("--stats")) {
      sampling = true;
      sampler = (async () => {
        while (sampling) {
          try {
            const lines = await docker([
              "stats",
              "--no-stream",
              "--format",
              "{{json .}}",
              backend,
              ...(pg ? [pg] : []),
            ]);
            for (const line of lines.split("\n")) {
              const sample = JSON.parse(line);
              resourceSamples.push({
                at: new Date().toISOString(),
                role: sample.Name === name ? "backend" : "postgres",
                ...sample,
              });
            }
          } catch (e) {
            resourceSamples.push({
              at: new Date().toISOString(),
              error: String(e),
            });
          }
          if (sampling) await delay(3000);
        }
      })();
    }
    console.log(
      await run(
        "sudo",
        [
          ...enter,
          "AUDIT_ADMIN_KEY=" + key,
          "AUDIT_BACKEND_CONTAINER_ID=" + backend,
          "AUDIT_PG_CONTAINER_ID=" + (pg ?? ""),
          "AUDIT_DATABASE=" + name.replaceAll("-", "_"),
          process.execPath,
          fileURLToPath(import.meta.url),
          "--driver",
          "--out",
          out,
          "--engine",
          engine,
          "--seconds",
          String(seconds),
          "--delay-ms",
          String(handlerDelay),
          "--rate",
          String(messagesPerMinute),
          "--seed-docs",
          String(seedDocuments),
          "--max-sockets",
          String(maxSockets),
          "--write-mib",
          String(writeLimitMiB),
          "--backend-cpus",
          String(backendCpus),
          "--postgres-cpus",
          String(postgresCpus),
          "--site-slots",
          String(siteSlots),
        ],
        { cwd: fixture, timeout: (seconds * 3 + 360) * 1000 },
      ),
    );
    // All requests are acknowledged before this crash probe; assert exact tenant totals after recovery.
    const expected = JSON.parse(
      await readFile(path.join(out, "requests.json"), "utf8"),
    ).counts;
    for (const id of [backend, ...(pg ? [pg] : [])]) {
      const [owned] = JSON.parse(await docker(["inspect", id]));
      assert.equal(owned.Config.Labels["amage.candidate4"], owner);
      await docker(["kill", "--signal", "KILL", id]);
    }
    const crashAt = new Date().toISOString();
    if (pg) {
      await docker(["start", pg]);
      let up = false;
      for (let i = 0; i < 120; i++) {
        try {
          await docker([
            "exec",
            pg,
            "pg_isready",
            "-h",
            "127.0.0.1",
            "-U",
            "postgres",
          ]);
          up = true;
          break;
        } catch {}
        await delay(500);
      }
      assert(up, "PG crash recovery");
    }
    await docker(["start", backend]);
    let recovered = false;
    for (let i = 0; i < 180; i++) {
      try {
        if (
          (await docker([
            "exec",
            backend,
            "curl",
            "-fsS",
            "--max-time",
            "2",
            "http://127.0.0.1:3210/version",
          ])) === version
        ) {
          recovered = true;
          break;
        }
      } catch {}
      await delay(500);
    }
    assert(recovered, "backend crash recovery");
    const [restarted] = JSON.parse(await docker(["inspect", backend]));
    const enterAfter = [
      "-n",
      "nsenter",
      "-t",
      String(restarted.State.Pid),
      "-n",
      "-S",
      String(process.getuid()),
      "-G",
      String(process.getgid()),
      "--",
      "/usr/bin/env",
      "-i",
      "PATH=" + process.env.PATH,
      "CONVEX_CLI_REPO=" + cliRepo,
      "AUDIT_ADMIN_KEY=" + key,
    ];
    const queryCode =
      "const{ConvexHttpClient}=require(process.env.CONVEX_CLI_REPO+'/npm-packages/convex/dist/cjs/browser/index.js');const c=new ConvexHttpClient('http://127.0.0.1:3210');c.setAdminAuth(process.env.AUDIT_ADMIN_KEY);import(" +
      JSON.stringify(path.join(root, "bench/count-fixture.mjs")) +
      ").then(({countFixture})=>countFixture(c)).then(x=>process.stdout.write(JSON.stringify(x))).catch(e=>{console.error(e);process.exitCode=1})";
    const actual = JSON.parse(
      await run("sudo", [...enterAfter, process.execPath, "-e", queryCode]),
    );
    assert.deepEqual(actual, expected);
    let integrity;
    if (engine === "sqlite") {
      integrity = JSON.parse(
        await docker([
          "exec",
          backend,
          "node",
          "-e",
          "const{DatabaseSync}=require('node:sqlite');const db=new DatabaseSync('/convex/data/db.sqlite3',{readOnly:true});db.exec('PRAGMA busy_timeout=5000');console.log(JSON.stringify({check:db.prepare('PRAGMA quick_check').all(),mode:db.prepare('PRAGMA journal_mode').get()}));db.close()",
        ]),
      );
      assert.deepEqual(integrity.check, [{ quick_check: "ok" }]);
      assert.equal(Object.values(integrity.mode)[0], sqliteJournal);
    }
    await writeFile(
      path.join(out, "crash-recovery.json"),
      JSON.stringify(
        {
          signal: "SIGKILL",
          crashAt,
          recoveredAt: new Date().toISOString(),
          counts: actual,
          integrity,
          postgresAlsoKilled: Boolean(pg),
        },
        null,
        2,
      ),
    );
  } finally {
    sampling = false;
    if (sampler) await sampler;
    await writeFile(
      path.join(out, "resources.json"),
      JSON.stringify(resourceSamples, null, 2),
    );
    if (backend) {
      const [state] = JSON.parse(await docker(["inspect", backend]));
      await writeFile(
        path.join(out, "exit-state.json"),
        JSON.stringify(state.State, null, 2),
      );
    }
    if (backend) {
      const { stdout, stderr } = await exec(
        "sudo",
        [
          "-n",
          "docker",
          "--host",
          "unix:///var/run/docker.sock",
          "logs",
          backend,
        ],
        { maxBuffer: 128 << 20 },
      );
      await writeFile(path.join(out, "backend.log"), redact(stdout + stderr));
    }
    if (pg) {
      const [state] = JSON.parse(await docker(["inspect", pg]));
      await writeFile(
        path.join(out, "postgres-state.json"),
        JSON.stringify(
          {
            State: state.State,
            memory: state.HostConfig.Memory,
            tmpfs: state.HostConfig.Tmpfs,
          },
          null,
          2,
        ),
      );
      const { stdout, stderr } = await exec(
        "sudo",
        ["-n", "docker", "--host", "unix:///var/run/docker.sock", "logs", pg],
        { maxBuffer: 128 << 20 },
      );
      await writeFile(path.join(out, "postgres.log"), redact(stdout + stderr));
    }
    const ownedVolumes = [];
    for (const id of ids) {
      const [info] = JSON.parse(await docker(["inspect", id]));
      for (const mount of info.Mounts)
        if (mount.Type === "volume") ownedVolumes.push(mount.Name);
    }
    await writeFile(
      path.join(out, "volumes.json"),
      JSON.stringify(ownedVolumes),
    );
    for (const id of ids.reverse()) {
      const [info] = JSON.parse(await docker(["inspect", id]));
      assert.equal(info.Config.Labels["amage.candidate4"], owner);
      await docker(["rm", "-fv", id]);
    }
    for (const volume of ownedVolumes) {
      let exists = true;
      try {
        await docker(["volume", "inspect", volume]);
      } catch {
        exists = false;
      }
      assert(!exists, "anonymous volume leaked: " + volume);
    }
    const remaining = await docker([
      "ps",
      "-aq",
      "--filter",
      "label=amage.candidate4=" + owner,
    ]);
    assert.equal(remaining, "");
    await writeFile(
      path.join(out, "cleanup.json"),
      JSON.stringify({ owner, remainingContainers: remaining }),
    );
  }
}
