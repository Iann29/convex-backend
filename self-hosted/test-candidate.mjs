// Linux-only local acceptance check. Requires Docker via sudo -n and an already
// built Convex CLI checkout. No package installation, publication, or real data.
// CONVEX_CLI_REPO=/path/to/checkout node self-hosted/test-candidate.mjs IMAGE
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash, randomBytes, randomUUID } from "node:crypto";
import {
  cp,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const exec = promisify(execFile);
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const cpuset = process.env.CANDIDATE_CPUSET;
assert(cpuset, "Set CANDIDATE_CPUSET to allowed logical CPUs, e.g. 0");
const cliRepo = process.env.CONVEX_CLI_REPO;
assert(cliRepo, "Set CONVEX_CLI_REPO to the checkout containing the built CLI");
const cli = path.join(cliRepo, "npm-packages/convex/dist/cli.bundle.cjs");
const require = createRequire(import.meta.url);
const { ConvexHttpClient, ConvexClient } = require(
  path.join(cliRepo, "npm-packages/convex/dist/cjs/browser/index.js"),
);
const tag = process.argv[2];
assert(tag, "Pass a local candidate image tag or ID");
const owner = `amage-close-${randomUUID().slice(0, 8)}`;
const containers = [],
  volumes = [],
  secrets = [];
let network, fixture, websocket;
const digest = (bytes) => createHash("sha256").update(bytes).digest("hex");
const sanitize = (value) =>
  secrets.reduce(
    (s, secret) => s.replaceAll(secret, "[REDACTED]"),
    String(value),
  );

async function run(command, args, options = {}) {
  try {
    const result = await exec(command, args, {
      timeout: 120000,
      maxBuffer: 8 * 1024 * 1024,
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

async function ready(url, container, version) {
  await waitFor("backend ready", async () => {
    const [info] = JSON.parse(await docker(["inspect", container]));
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
  const schemaPath = path.join(fixture, "convex/schema.ts");
  const schema = await readFile(schemaPath, "utf8");
  assert(schema.includes("format: v.string(),\n  }),"));
  await writeFile(
    schemaPath,
    schema.replace(
      "format: v.string(),\n  }),",
      'format: v.string(),\n  }).searchIndex("search_body", { searchField: "body" }),',
    ),
  );
  await writeFile(
    path.join(fixture, "convex/acceptance.ts"),
    `
import { query, mutation } from "./_generated/server";
import { api } from "./_generated/api";
import { v } from "convex/values";
export const search = query({ args: { text: v.string() }, handler: (ctx, args) =>
  ctx.db.query("messages").withSearchIndex("search_body", q => q.search("body", args.text)).collect() });
export const scheduleNode = mutation({ args: { at: v.number(), body: v.string() }, handler: (ctx, args) =>
  ctx.scheduler.runAt(args.at, api.node.check, { body: args.body }) });
`,
  );
  await writeFile(
    path.join(fixture, "convex/node.ts"),
    `
"use node";
import { createHash } from "node:crypto";
import { action } from "./_generated/server";
import { api } from "./_generated/api";
import { v } from "convex/values";
export const check = action({ args: { body: v.string() }, handler: async (ctx, args) => {
  const sha256 = createHash("sha256").update(args.body).digest("hex");
  await ctx.runMutation(api.messages.sendMessage, { body: args.body, author: "node-synthetic" });
  const messages = await ctx.runQuery(api.messages.list, {});
  const storageId = await ctx.storage.store(new Blob([args.body], { type: "text/plain" }));
  const blob = await ctx.storage.get(storageId);
  if (!blob || await blob.text() !== args.body) throw new Error("Node storage roundtrip failed");
  return { version: process.version, sha256, storageId, id: messages.find(m => m.body === args.body)?._id };
} });
export const fail = action({ args: {}, handler: async () => { throw new Error("AMAGE_EXPECTED_NODE_FAILURE"); } });
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
    RUST_LOG: "warn",
    // Compile-time identity must take precedence over a runtime override.
    CONVEX_RELEASE_VERSION_DEV: "must-not-override-candidate",
  };
  const db = name.replaceAll("-", "_");
  if (engine === "postgres") {
    await docker(["exec", pg.id, "createdb", "-U", "postgres", db]);
    env.POSTGRES_URL = `postgresql://postgres:${pg.password}@${pg.ip}:5432`;
    env.DO_NOT_REQUIRE_SSL = "1";
  }
  const container = await docker([
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
    ...Object.entries(env).flatMap(([key, value]) => ["-e", `${key}=${value}`]),
    image,
  ]);
  containers.push(container);
  await docker(["start", container]);
  console.log(JSON.stringify({ stage: "starting", engine, name, cpuset }));
  await ready(url, container, version);
  assert.equal(
    await docker(["exec", container, "./convex-local-backend", "--version"]),
    `local_backend ${version}`,
  );
  const [info] = JSON.parse(await docker(["inspect", container]));
  assert.deepEqual(info.HostConfig.PortBindings, {});
  const routes = await docker(["exec", container, "cat", "/proc/net/route"]);
  assert(
    !routes
      .split("\n")
      .slice(1)
      .some((line) => line.trim().split(/\s+/)[1] === "00000000"),
    "Internal container must have no default route",
  );
  const key = await docker(["exec", container, "./generate_admin_key.sh"]);
  secrets.push(key);
  // Execute the installed host CLI in this isolated network namespace, under
  // the original uid. Explicit env avoids existing deployment config/telemetry.
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
  const cliVersion = await run("sudo", [...cliArgs, "--version"], {
    cwd: fixture,
  });
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
  console.log(
    JSON.stringify({
      stage: "deployed",
      engine,
      cliVersion,
      version,
      internalNetwork: true,
    }),
  );
  const client = new ConvexHttpClient(url);
  client.setAdminAuth(key);
  const body = `amagecanary ${randomUUID()}`;
  const author = "local-synthetic";
  let observed = [];
  websocket = new ConvexClient(url, { webSocketConstructor: WebSocket });
  websocket.onUpdate("messages:list", {}, (result) => {
    observed = result;
  });
  await client.mutation("messages:sendMessage", { body, author });
  await waitFor("realtime before restart", () =>
    observed.some((row) => row.body === body),
  );
  const before = (await client.query("messages:list", {})).find(
    (row) => row.body === body,
  );
  assert(before);
  await waitFor("search before restart", async () =>
    (await client.query("acceptance:search", { text: "amagecanary" })).some(
      (row) => row._id === before._id,
    ),
  );

  const payload = Buffer.from("amage-file-smoke\n".repeat(2048));
  const uploadUrl = await client.mutation("messages:generateUploadUrl", {});
  assert.equal(new URL(uploadUrl).origin, url);
  const upload = await fetch(uploadUrl, {
    method: "POST",
    headers: { "Content-Type": "application/octet-stream" },
    body: payload,
    signal: AbortSignal.timeout(10000),
  });
  assert(upload.ok);
  const fileId = (await upload.json()).storageId;
  await client.mutation("messages:sendImage", { storageId: fileId, author });
  async function verifyFile(id, bytes) {
    const fileUrl = await client.query("admin:getFileUrl", { id });
    assert.equal(new URL(fileUrl).origin, url);
    const response = await fetch(fileUrl, {
      signal: AbortSignal.timeout(10000),
    });
    assert(response.ok);
    assert.equal(
      digest(Buffer.from(await response.arrayBuffer())),
      digest(bytes),
    );
  }
  await verifyFile(fileId, payload);
  await assert.rejects(
    client.action("node:fail", {}),
    /AMAGE_EXPECTED_NODE_FAILURE/,
  );
  const nodeBody = `${body} node-before`;
  const nodeBefore = await client.action("node:check", { body: nodeBody });
  assert.equal(nodeBefore.version, "v22.22.2");
  assert.equal(nodeBefore.sha256, digest(nodeBody));
  assert.equal(typeof nodeBefore.id, "string");
  await verifyFile(nodeBefore.storageId, nodeBody);
  console.log(
    JSON.stringify({
      stage: "node-passed",
      engine,
      nodeVersion: nodeBefore.version,
      callbacks: ["mutation", "query", "storage.store", "storage.get"],
      errorRecovery: true,
    }),
  );

  const at = Date.now() + 45000;
  const mutationJob = await client.mutation("messages:scheduleMessage", {
    delay: 45,
    body: `${body} scheduled-mutation`,
    author,
  });
  const nodeJob = await client.mutation("acceptance:scheduleNode", {
    at,
    body: `${body} scheduled-node`,
  });
  const pending = await client.query("admin:listScheduledSends", {});
  for (const id of [mutationJob, nodeJob])
    assert.equal(pending.find((job) => job._id === id)?.state.kind, "pending");
  const dueTimes = [mutationJob, nodeJob].map(
    (id) => pending.find((job) => job._id === id).scheduledTime,
  );
  await docker(["stop", container]);
  assert(
    Date.now() < Math.min(...dueTimes),
    "Backend must stop before jobs become due",
  );
  if (engine === "postgres") {
    await docker(["restart", pg.id]);
    await waitFor("Postgres restart", async () => {
      try {
        await docker([
          "exec",
          pg.id,
          "pg_isready",
          "-h",
          "127.0.0.1",
          "-U",
          "postgres",
        ]);
        return true;
      } catch {
        return false;
      }
    });
  }
  console.log(
    JSON.stringify({
      stage: "offline-until-jobs-overdue",
      engine,
      jobIds: [mutationJob, nodeJob],
      dueTimes,
    }),
  );
  await delay(Math.max(0, Math.max(...dueTimes) + 1000 - Date.now()));
  await docker(["start", container]);
  await ready(url, container, version);
  assert.deepEqual(
    (await client.query("messages:list", {})).find(
      (row) => row._id === before._id,
    ),
    before,
  );
  assert(
    (await client.query("acceptance:search", { text: "amagecanary" })).some(
      (row) => row._id === before._id,
    ),
  );
  await verifyFile(fileId, payload);
  await verifyFile(nodeBefore.storageId, nodeBody);
  assert.equal(
    (await client.query("admin:listFiles", {})).find(
      (file) => file._id === fileId,
    )?.size,
    payload.length,
  );
  const afterBody = `${body} node-after`;
  const nodeAfter = await client.action("node:check", { body: afterBody });
  assert.equal(nodeAfter.version, nodeBefore.version);
  assert.equal(nodeAfter.sha256, digest(afterBody));
  assert.equal(typeof nodeAfter.id, "string");
  await verifyFile(nodeAfter.storageId, afterBody);
  await waitFor(
    "same realtime subscription reconnects and receives new writes",
    () => observed.some((row) => row._id === nodeAfter.id),
  );
  await waitFor("both overdue jobs succeed", async () => {
    const jobs = await client.query("admin:listScheduledSends", {});
    return [mutationJob, nodeJob].every(
      (id) => jobs.find((job) => job._id === id)?.state.kind === "success",
    );
  });
  const messages = await client.query("messages:list", {});
  assert.equal(
    messages.find((row) => row._id === nodeBefore.id)?.body,
    nodeBody,
  );
  for (const suffix of ["scheduled-mutation", "scheduled-node"])
    assert.equal(
      messages.filter((row) => row.body === `${body} ${suffix}`).length,
      1,
    );
  // Exercise export completion, local object attributes and ZIP download through
  // the real CLI. The large-file allocation regression is a bounded Rust test.
  const exportPath = path.join(fixture, `${engine}-snapshot.zip`);
  // Restart changed the PID used by nsenter; keep using the isolated namespace.
  const [exportInfo] = JSON.parse(await docker(["inspect", container]));
  assert(exportInfo.State.Running);
  cliArgs[cliArgs.indexOf("-t") + 1] = String(exportInfo.State.Pid);
  await run(
    "sudo",
    [...cliArgs, "export", "--include-file-storage", "--path", exportPath],
    { cwd: fixture },
  );
  // Independently read/CRC-check all entries. Do not disable a decoder's
  // zip-bomb protection to accommodate streaming ZIP64 descriptors.
  const snapshot = JSON.parse(
    await run("python3", [
      "-c",
      `
import hashlib, json, sys, zipfile
with zipfile.ZipFile(sys.argv[1]) as archive:
    entries = archive.infolist()
    assert len(entries) <= 1000
    assert sum(entry.file_size for entry in entries) <= 16 * 1024 * 1024
    assert archive.testzip() is None, "ZIP CRC check failed"
    documents = [json.loads(line) for line in archive.read("messages/documents.jsonl").splitlines() if line]
    prefix = "_storage/" + sys.argv[2]
    files = [entry for entry in entries if entry.filename == prefix or entry.filename.startswith(prefix + ".")]
    assert len(files) == 1, "Export must include exactly one matching stored file"
    print(json.dumps({"documents": documents, "fileSha256": hashlib.sha256(archive.read(files[0])).hexdigest(), "entries": len(entries)}))
`,
      exportPath,
      fileId,
    ]),
  );
  await run("bsdtar", ["-xOf", exportPath], { maxBuffer: 16 * 1024 * 1024 });
  assert.deepEqual(
    snapshot.documents.find((row) => row._id === before._id),
    before,
  );
  assert.equal(snapshot.fileSha256, digest(payload));
  console.log(
    JSON.stringify({
      stage: "snapshot-export-passed",
      engine,
      zipIntegrity: true,
      documentIdentity: true,
      validators: ["python-zipfile", "libarchive"],
      entries: snapshot.entries,
      fileSha256: snapshot.fileSha256,
    }),
  );
  if (engine === "postgres") {
    const count = await docker([
      "exec",
      pg.id,
      "psql",
      "-U",
      "postgres",
      "-d",
      db,
      "-Atc",
      "SELECT count(*) FROM documents;",
    ]);
    assert(Number(count) > 0);
    console.log(
      JSON.stringify({
        stage: "postgres-persistence",
        physicalDocumentRows: Number(count),
      }),
    );
  } else
    await docker(["exec", container, "test", "-s", "/convex/data/db.sqlite3"]);
  console.log(
    JSON.stringify({
      stage: "passed",
      engine,
      imageId: image,
      persistedId: before._id,
      fileSha256: digest(payload),
      nodeActionsBeforeAfter: true,
      realtimeReconnected: true,
      searchBeforeAfter: true,
      overdueMutationAndNodeJobs: true,
    }),
  );
  await websocket.close();
  websocket = undefined;
  await docker(["stop", container]);
  const [stopped] = JSON.parse(await docker(["inspect", container]));
  assert.equal(stopped.State.OOMKilled, false);
}

try {
  const [image] = JSON.parse(await docker(["image", "inspect", tag]));
  const labels = image.Config.Labels;
  const manifest = JSON.parse(
    await readFile(
      path.join(
        root,
        "self-hosted",
        process.env.CANDIDATE_MANIFEST ?? "amage-candidate4.json",
      ),
      "utf8",
    ),
  );
  const base = manifest.base;
  const { stdout: patch } = await exec(
    "git",
    [
      "-C",
      root,
      "diff",
      "--binary",
      "--no-ext-diff",
      "--no-color",
      "--src-prefix=a/",
      "--dst-prefix=b/",
      "--abbrev=9",
      base,
      "--",
      "crates",
      "self-hosted/docker-build/Dockerfile.backend",
    ],
    { encoding: "buffer" },
  );
  assert.equal(digest(patch), manifest.patchSha256);
  assert.equal(labels["org.amage.base-revision"], base);
  assert.equal(labels["org.opencontainers.image.revision"], base);
  assert.equal(labels["org.amage.patch-sha256"], digest(patch));
  const version = labels["org.opencontainers.image.version"];
  assert(
    version.includes(
      `base.${base.slice(0, 12)}.patch.${digest(patch).slice(0, 12)}`,
    ),
  );
  assert(image.Config.Env.includes("SEARCHLIGHT_DISK_CACHE_MIB=2048"));
  console.log(
    JSON.stringify({
      imageId: image.Id,
      version,
      patchSha256: digest(patch),
      owner,
    }),
  );
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
      await docker([
        "exec",
        pgId,
        "pg_isready",
        "-h",
        "127.0.0.1",
        "-U",
        "postgres",
      ]);
      return true;
    } catch {
      return false;
    }
  });
  console.log(
    JSON.stringify({
      postgresVersion: await docker(["exec", pgId, "postgres", "--version"]),
      internalNetwork: net.Name,
    }),
  );
  for (const [index, engine] of ["sqlite", "postgres"].entries())
    await runCase(
      image.Id,
      version,
      { id: pgId, ip: pgIp, password },
      engine,
      `${prefix}.${index + 3}`,
    );
  console.log("ALL LOCAL ACCEPTANCE CHECKS PASSED");
} catch (error) {
  console.error(sanitize(error.stack || error));
  process.exitCode = 1;
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
    ]);
    console.error(sanitize(logs.stdout + logs.stderr));
  }
} finally {
  try {
    await websocket?.close();
  } catch (error) {
    console.error(sanitize(error.message));
    process.exitCode = 1;
  }
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
  console.log(
    "Cleanup completed for this run's containers, volumes, network and fixture.",
  );
}
