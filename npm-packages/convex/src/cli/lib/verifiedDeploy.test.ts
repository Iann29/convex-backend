import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import http from "node:http";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import zlib from "node:zlib";
import { Crash, WatchContext } from "./watch.js";
import { Span } from "./tracing.js";
import {
  collectCandidateFiles,
  fetchVerifiedDeployCapability,
  runVerifiedPush,
  VerifiedDeployCapability,
} from "./verifiedDeploy.js";

type Recorded = {
  path: string;
  headers: http.IncomingHttpHeaders;
  body: any;
};

type Fake = {
  url: string;
  requests: Recorded[];
  close: (callback?: () => void) => void;
};

// One in-process HTTP server; `respond` maps a request path to a JSON reply.
async function fake(
  respond: (path: string, body: any) => { status: number; body: any },
): Promise<Fake> {
  const requests: Recorded[] = [];
  const server = http.createServer((req, res) => {
    if (req.method === "HEAD") {
      // Something on the host probes freshly opened ports with `HEAD /`;
      // the CLI never sends HEAD, so it's noise for these assertions.
      res.writeHead(404).end();
      return;
    }
    const chunks: Buffer[] = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      let raw = Buffer.concat(chunks);
      if (req.headers["content-encoding"] === "br") {
        raw = zlib.brotliDecompressSync(raw);
      }
      const body = raw.length > 0 ? JSON.parse(raw.toString("utf8")) : null;
      requests.push({ path: req.url!, headers: req.headers, body });
      const reply = respond(req.url!, body);
      res.writeHead(reply.status, { "Content-Type": "application/json" });
      res.end(JSON.stringify(reply.body));
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as { port: number };
  return {
    url: `http://127.0.0.1:${port}`,
    requests,
    close: (callback?: () => void) => server.close(callback),
  };
}

const envelope = {
  format: 1,
  protocol: "amage.verified-deploy/1",
  target: { instanceName: "inst", project: "ledger", deployment: "dev" },
  requestSha256: "ab".repeat(32),
  policy: { id: "c7", version: 3, sha256: "cd".repeat(32) },
  candidateId: "cand-1",
  receiptSha256: "ef".repeat(32),
  issuedAt: "2026-09-20T18:00:00Z",
  expiresAt: "2026-09-20T18:15:00Z",
  signature: "c2lnbmF0dXJl",
};

const artifactRequest = {
  dryRun: false,
  functions: "convex/",
  appDefinition: {
    definition: null,
    dependencies: [],
    schema: null,
    changedModules: [
      {
        path: "ledger/model.js",
        source: "export default 1;",
        environment: "isolate",
      },
    ],
    unchangedModuleHashes: [],
    udfServerVersion: "1.45.0",
  },
  componentDefinitions: [],
  nodeDependencies: [],
};

const invariants = [
  { id: "tenant_isolation", version: 1, status: "accepted" },
  { id: "idempotency", version: 1, status: "accepted" },
];

const authorized = {
  format: 1,
  status: "authorized",
  candidateId: "cand-1",
  deployAllowed: true,
  rollout: "auto",
  policy: envelope.policy,
  invariants,
  requestSha256: envelope.requestSha256,
  receiptSha256: envelope.receiptSha256,
  request: artifactRequest,
  amageVerified: envelope,
  generated: { "ledger/model.js": "export default 1;\n" },
};

const startPushResponse = {
  environmentVariables: {},
  externalDepsId: null,
  componentDefinitionPackages: {},
  appAuth: [],
  analysis: {},
  app: {
    definitionPath: "",
    componentPath: "",
    args: {},
    childComponents: {},
    httpRoutes: { httpModuleRoutes: null, mounts: [] },
    exports: {},
  },
  schemaChange: { allocatedComponentIds: {}, schemaIds: {} },
};

const finishPushDiff = {
  authDiff: { added: [], removed: [] },
  definitionDiffs: {},
  componentDiffs: {},
};

function protectedBackend(supervisorUrl: string) {
  return fake((url) => {
    switch (url) {
      case "/api/amage/verified_deploy":
        return {
          status: 200,
          body: {
            protocol: "amage.verified-deploy/1",
            supported: true,
            required: true,
            target: { instanceName: "inst" },
            verifierKeyId: "k",
            supervisorUrl,
          },
        };
      case "/api/deploy2/start_push":
        return { status: 200, body: startPushResponse };
      case "/api/deploy2/wait_for_schema":
        return { status: 200, body: { type: "complete" } };
      case "/api/deploy2/finish_push":
        return { status: 200, body: finishPushDiff };
      default:
        return { status: 404, body: { code: "NotFound", message: url } };
    }
  });
}

function makeProject(): { root: string; convexDir: string } {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "verified-deploy-"));
  const convexDir = path.join(root, "convex");
  const write = (rel: string, contents: string) => {
    const abs = path.join(convexDir, rel);
    fs.mkdirSync(path.dirname(abs), { recursive: true });
    fs.writeFileSync(abs, contents);
  };
  write("schema.ts", "export default {};\n");
  write("ledger/records.ts", "export const x = 1;\n");
  write("ledger/model.bend", "def main = 1\n");
  write("ledger/contract.json", '{"id":"c7"}\n');
  write("ledger/model.js", "export default 0;\n");
  write("_generated/api.d.ts", "export {};\n");
  write("node_modules/dep/index.js", "module.exports = {};\n");
  write("tsconfig.json", "{}\n");
  write("types.d.ts", "export {};\n");
  write("README.md", "# nope\n");
  return { root, convexDir };
}

function ctxFor() {
  return new WatchContext(false, null, true);
}

function optionsFor(url: string) {
  return {
    adminKey: "admin",
    url,
    deploymentName: null,
    dryRun: false,
    verbose: false,
    debug: false,
    typecheck: "disable" as const,
    codegen: false,
    debugNodeApis: false,
  };
}

function rootComponentFor(convexDir: string) {
  return {
    isRoot: true,
    path: convexDir,
    definitionPath: path.join(convexDir, "convex.config.ts"),
    isRootWithoutConfig: true,
  };
}

async function crashOf(promise: Promise<unknown>): Promise<Crash> {
  try {
    await promise;
  } catch (e) {
    expect(e).toBeInstanceOf(Crash);
    return e as Crash;
  }
  throw new Error("expected a crash");
}

const requiredCapability: VerifiedDeployCapability = {
  protocol: "amage.verified-deploy/1",
  required: true,
  legacyBackend: false,
};

describe("verified deploys", () => {
  const closers: (() => void)[] = [];
  let project: { root: string; convexDir: string };
  const env = { ...process.env };

  beforeEach(() => {
    project = makeProject();
    process.env.AMAGE_CODING_CREDENTIAL = "cred";
    delete process.env.AMAGE_SUPERVISOR_URL;
  });

  afterEach(() => {
    for (const close of closers.splice(0)) {
      close();
    }
    fs.rmSync(project.root, { recursive: true, force: true });
    process.env = { ...env };
    vi.restoreAllMocks();
  });

  async function supervisor(
    respond: (op: string, body: any) => { status: number; body: any },
  ) {
    const s = await fake((url, body) => respond(url.replace("/v1/", ""), body));
    closers.push(s.close);
    return s;
  }

  async function backend(supervisorUrl: string) {
    const b = await protectedBackend(supervisorUrl);
    closers.push(b.close);
    return b;
  }

  test("capability 404 means a legacy, unprotected backend", async () => {
    const b = await fake(() => ({ status: 404, body: null }));
    closers.push(b.close);
    const cap = await fetchVerifiedDeployCapability(ctxFor(), {
      url: b.url,
      adminKey: "admin",
    });
    expect(cap).toEqual({
      protocol: "amage.verified-deploy/1",
      required: false,
      legacyBackend: true,
    });
  });

  test("capability announces protection and the supervisor", async () => {
    const b = await backend("http://supervisor.test");
    const cap = await fetchVerifiedDeployCapability(ctxFor(), {
      url: b.url,
      adminKey: "admin",
    });
    expect(cap.required).toBe(true);
    expect(cap.legacyBackend).toBe(false);
    expect(cap.supervisorUrl).toBe("http://supervisor.test");
  });

  test("an unknown protocol on a protected backend is fatal", async () => {
    const b = await fake(() => ({
      status: 200,
      body: { protocol: "amage.verified-deploy/2", required: true },
    }));
    closers.push(b.close);
    const crash = await crashOf(
      fetchVerifiedDeployCapability(ctxFor(), { url: b.url, adminKey: "a" }),
    );
    expect(crash.errorType).toBe("fatal");
  });

  test("collectCandidateFiles reads .bend/.ts/.js/.json through ctx.fs", async () => {
    const ctx = ctxFor();
    const files = await collectCandidateFiles(ctx, project.convexDir);
    expect(Object.keys(files)).toEqual([
      "ledger/contract.json",
      "ledger/model.bend",
      "ledger/model.js",
      "ledger/records.ts",
      "schema.ts",
    ]);
    expect(files["ledger/model.bend"]).toBe("def main = 1\n");
    const observations = ctx.fs.finalize();
    expect(observations).not.toBe("invalidated");
    if (observations !== "invalidated") {
      expect(
        observations.files.has(
          path.join(project.convexDir, "ledger/records.ts"),
        ),
      ).toBe(true);
    }
  });

  test("a missing coding credential is fatal before anything is sent", async () => {
    delete process.env.AMAGE_CODING_CREDENTIAL;
    const s = await supervisor(() => ({ status: 200, body: authorized }));
    const b = await backend(s.url);
    const crash = await crashOf(
      runVerifiedPush(
        ctxFor(),
        Span.noop(),
        rootComponentFor(project.convexDir),
        new Map(),
        optionsFor(b.url),
        { ...requiredCapability, supervisorUrl: s.url },
      ),
    );
    expect(crash.errorType).toBe("fatal");
    expect(s.requests).toHaveLength(0);
    expect(b.requests).toHaveLength(0);
  });

  test("a passing build forwards the artifact and envelope unchanged", async () => {
    const s = await supervisor((op) => {
      switch (op) {
        case "build":
          return { status: 200, body: authorized };
        case "report":
          return { status: 200, body: { format: 1, status: "recorded" } };
        default:
          return { status: 400, body: { error: op } };
      }
    });
    const b = await backend(s.url);
    const stdout = vi.spyOn(console, "log").mockImplementation(() => {});
    const ctx = ctxFor();
    const result = await runVerifiedPush(
      ctx,
      Span.noop(),
      rootComponentFor(project.convexDir),
      new Map(),
      { ...optionsFor(b.url), verifiedJson: true, message: "release 4" },
      { ...requiredCapability, supervisorUrl: s.url },
    );
    expect(result.finishPushResponse).toEqual(finishPushDiff);

    const build = s.requests[0];
    expect(build.path).toBe("/v1/build");
    expect(build.headers.authorization).toBe("Bearer cred");
    expect(Object.keys(build.body)).toEqual(["files"]);
    expect(Object.keys(build.body.files)).toContain("ledger/model.bend");

    const paths = b.requests.map((r) => r.path);
    expect(paths).toEqual([
      "/api/deploy2/start_push",
      "/api/deploy2/wait_for_schema",
      "/api/deploy2/finish_push",
    ]);
    const startPush = b.requests[0].body;
    expect(startPush.amageVerified).toEqual(envelope);
    expect(startPush.adminKey).toBe("admin");
    const { adminKey: _adminKey, amageVerified: _env, ...rest } = startPush;
    expect(rest).toEqual(artifactRequest);
    expect(b.requests[2].body.message).toBe(
      `amage-verified candidate=cand-1 policy=c7@3 receipt=${envelope.receiptSha256} release 4`,
    );

    expect(
      fs.readFileSync(path.join(project.convexDir, "ledger/model.js"), "utf8"),
    ).toBe("export default 1;\n");
    expect(ctx.fs.finalize()).not.toBe("invalidated");

    expect(s.requests[1].path).toBe("/v1/report");
    expect(s.requests[1].body).toMatchObject({
      candidateId: "cand-1",
      outcome: "deployed",
    });

    const events = stdout.mock.calls.map((call) => JSON.parse(call[0]));
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      event: "verification",
      status: "passed",
      candidateId: "cand-1",
      policy: envelope.policy,
      requestSha256: envelope.requestSha256,
      receiptSha256: envelope.receiptSha256,
      deploy: { status: "deployed" },
    });
    expect(events[0].invariants).toEqual(invariants);
  });

  test("a failing build sends nothing to the backend", async () => {
    const s = await supervisor(() => ({
      status: 200,
      body: {
        format: 1,
        status: "rejected",
        candidateId: "cand-9",
        deployAllowed: false,
        policy: envelope.policy,
        invariants: [
          { id: "idempotency", version: 1, status: "accepted" },
          {
            id: "tenant_isolation",
            version: 1,
            status: "rejected",
            diagnostic: {
              kind: "proof_error",
              raw: "LAW_tenant.isolated: type mismatch\nexpected Tenant\nfound Any\nmore",
            },
          },
        ],
      },
    }));
    const b = await backend(s.url);
    const stderr: string[] = [];
    vi.spyOn(process.stderr, "write").mockImplementation((chunk: any) => {
      stderr.push(String(chunk));
      return true;
    });
    const crash = await crashOf(
      runVerifiedPush(
        ctxFor(),
        Span.noop(),
        rootComponentFor(project.convexDir),
        new Map(),
        optionsFor(b.url),
        { ...requiredCapability, supervisorUrl: s.url },
      ),
    );
    expect(crash.errorType).toBe("invalid filesystem data");
    expect(b.requests).toHaveLength(0);
    expect(s.requests.map((r) => r.path)).toEqual(["/v1/build"]);
    const output = stderr.join("");
    expect(output).toContain("Verification failed (c7@3)");
    expect(output).toContain("tenant_isolation@1 — LAW_tenant.isolated");
    expect(output).toContain("Previous verified deployment remains active");
    expect(
      fs.readFileSync(path.join(project.convexDir, "ledger/model.js"), "utf8"),
    ).toBe("export default 0;\n");
  });

  test("deployAllowed=false blocks even when the status looks good", async () => {
    const s = await supervisor(() => ({
      status: 200,
      body: { ...authorized, deployAllowed: false },
    }));
    const b = await backend(s.url);
    const crash = await crashOf(
      runVerifiedPush(
        ctxFor(),
        Span.noop(),
        rootComponentFor(project.convexDir),
        new Map(),
        optionsFor(b.url),
        { ...requiredCapability, supervisorUrl: s.url },
      ),
    );
    expect(crash.errorType).toBe("fatal");
    expect(b.requests).toHaveLength(0);
  });

  test("a policy change is blocked and lists the files", async () => {
    const s = await supervisor(() => ({
      status: 200,
      body: {
        format: 1,
        status: "rejected",
        candidateId: "cand-3",
        deployAllowed: false,
        kind: "policy_change_requires_policy_authority",
        files: ["ledger/contract.json", "ledger/LAW_tenant.bend"],
      },
    }));
    const b = await backend(s.url);
    const stderr: string[] = [];
    vi.spyOn(process.stderr, "write").mockImplementation((chunk: any) => {
      stderr.push(String(chunk));
      return true;
    });
    const stdout = vi.spyOn(console, "log").mockImplementation(() => {});
    const crash = await crashOf(
      runVerifiedPush(
        ctxFor(),
        Span.noop(),
        rootComponentFor(project.convexDir),
        new Map(),
        { ...optionsFor(b.url), verifiedJson: true },
        { ...requiredCapability, supervisorUrl: s.url },
      ),
    );
    expect(crash.errorType).toBe("invalid filesystem data");
    expect(b.requests).toHaveLength(0);
    const output = stderr.join("");
    expect(output).toContain("Policy change requires policy authority.");
    expect(output).toContain("ledger/contract.json");
    expect(output).toContain("ledger/LAW_tenant.bend");
    expect(JSON.parse(stdout.mock.calls[0][0])).toMatchObject({
      status: "blocked",
      candidateId: "cand-3",
      deploy: { status: "skipped" },
    });
  });

  test("an unreachable supervisor is transient and sends nothing", async () => {
    // A port that was just released: nothing listens there.
    const closed = await fake(() => ({ status: 200, body: null }));
    await new Promise<void>((resolve) => closed.close(() => resolve()));
    process.env.AMAGE_SUPERVISOR_URL = closed.url;
    const b = await backend(closed.url);
    const stderr: string[] = [];
    vi.spyOn(process.stderr, "write").mockImplementation((chunk: any) => {
      stderr.push(String(chunk));
      return true;
    });
    const crash = await crashOf(
      runVerifiedPush(
        ctxFor(),
        Span.noop(),
        rootComponentFor(project.convexDir),
        new Map(),
        optionsFor(b.url),
        requiredCapability,
      ),
    );
    expect(crash.errorType).toBe("transient");
    expect(b.requests).toHaveLength(0);
    expect(stderr.join("")).toContain(
      `Verifier unavailable at ${closed.url}/v1/build (ECONNREFUSED); nothing was deployed.`,
    );
  });

  test("a rejected credential is fatal", async () => {
    const s = await supervisor(() => ({
      status: 401,
      body: { error: "unknown credential" },
    }));
    const b = await backend(s.url);
    const crash = await crashOf(
      runVerifiedPush(
        ctxFor(),
        Span.noop(),
        rootComponentFor(project.convexDir),
        new Map(),
        optionsFor(b.url),
        { ...requiredCapability, supervisorUrl: s.url },
      ),
    );
    expect(crash.errorType).toBe("fatal");
    expect(b.requests).toHaveLength(0);
  });

  test("dry runs are refused on protected deployments", async () => {
    const s = await supervisor(() => ({ status: 200, body: authorized }));
    const b = await backend(s.url);
    const crash = await crashOf(
      runVerifiedPush(
        ctxFor(),
        Span.noop(),
        rootComponentFor(project.convexDir),
        new Map(),
        { ...optionsFor(b.url), dryRun: true },
        { ...requiredCapability, supervisorUrl: s.url },
      ),
    );
    expect(crash.errorType).toBe("fatal");
    expect(s.requests).toHaveLength(0);
    expect(b.requests).toHaveLength(0);
  });

  test("manual rollout waits for the authority, never authorizing itself", async () => {
    const ready = {
      format: 1,
      status: "ready",
      candidateId: "cand-1",
      deployAllowed: false,
      rollout: "manual",
      policy: envelope.policy,
      invariants,
    };
    const s = await supervisor((op) => {
      switch (op) {
        case "build":
          return { status: 200, body: ready };
        case "candidate":
          return { status: 200, body: authorized };
        default:
          return { status: 200, body: { format: 1, status: "recorded" } };
      }
    });
    const b = await backend(s.url);
    await runVerifiedPush(
      ctxFor(),
      Span.noop(),
      rootComponentFor(project.convexDir),
      new Map(),
      { ...optionsFor(b.url), verifiedWait: 30 },
      { ...requiredCapability, supervisorUrl: s.url },
    );
    expect(s.requests.map((r) => r.path)).toEqual([
      "/v1/build",
      "/v1/candidate",
      "/v1/report",
    ]);
    expect(s.requests[1].body).toEqual({ candidateId: "cand-1" });
    expect(b.requests[0].path).toBe("/api/deploy2/start_push");
    expect(b.requests[0].body.amageVerified).toEqual(envelope);
  }, 10_000);

  test("--verified-wait 0 exits without waiting", async () => {
    const s = await supervisor(() => ({
      status: 200,
      body: {
        format: 1,
        status: "ready",
        candidateId: "cand-1",
        deployAllowed: false,
        rollout: "manual",
      },
    }));
    const b = await backend(s.url);
    const crash = await crashOf(
      runVerifiedPush(
        ctxFor(),
        Span.noop(),
        rootComponentFor(project.convexDir),
        new Map(),
        { ...optionsFor(b.url), verifiedWait: 0 },
        { ...requiredCapability, supervisorUrl: s.url },
      ),
    );
    expect(crash.errorType).toBe("invalid filesystem data");
    expect(s.requests.map((r) => r.path)).toEqual(["/v1/build"]);
    expect(b.requests).toHaveLength(0);
  });

  test("generated files outside <member>/model.js are refused", async () => {
    const s = await supervisor(() => ({
      status: 200,
      body: { ...authorized, generated: { "schema.ts": "export default 2;" } },
    }));
    const b = await backend(s.url);
    const crash = await crashOf(
      runVerifiedPush(
        ctxFor(),
        Span.noop(),
        rootComponentFor(project.convexDir),
        new Map(),
        optionsFor(b.url),
        { ...requiredCapability, supervisorUrl: s.url },
      ),
    );
    expect(crash.errorType).toBe("fatal");
    expect(b.requests).toHaveLength(0);
    expect(
      fs.readFileSync(path.join(project.convexDir, "schema.ts"), "utf8"),
    ).toBe("export default {};\n");
  });
});
