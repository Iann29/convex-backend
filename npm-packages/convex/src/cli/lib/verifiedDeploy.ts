import path from "path";
import { z } from "zod";
import { chalkStderr } from "chalk";
import { Context } from "../../bundler/context.js";
import { consistentPathSort, withTmpDir } from "../../bundler/fs.js";
import {
  changeSpinner,
  logError,
  logFailure,
  logFinishedStep,
  logMessage,
  logOutput,
  logVerbose,
  logWarning,
  showSpinner,
} from "../../bundler/log.js";
import { DeploymentType } from "./api.js";
import { doFinalComponentCodegen } from "./codegen.js";
import { ComponentDirectory } from "./components/definition/directoryStructure.js";
import { handlePushConfigError } from "./config.js";
import { finishPush, startPush, waitForSchema } from "./deploy2.js";
import { FinishPushDiff } from "./deployApi/finishPush.js";
import { startPushRequest, StartPushResponse } from "./deployApi/startPush.js";
import { looseObject } from "./deployApi/utils.js";
import { Span } from "./tracing.js";
import { TypeCheckMode, typeCheckFunctionsInMode } from "./typecheck.js";
import {
  CODING_CREDENTIAL_ENV_VAR_NAME,
  deploymentFetch,
  logAndHandleFetchError,
  SUPERVISOR_URL_ENV_VAR_NAME,
  ThrowingFetchError,
} from "./utils/utils.js";

// Protected deployments (`amage.verified-deploy/2`) only install code that the
// supervisor verified and signed. The CLI's part is transport and UX: it asks
// the backend whether it is protected, sends the candidate sources to the
// supervisor, forwards the returned artifact untouched with the supervisor's
// receipt token in place of the admin key, and never signs, approves, or
// bypasses anything.

export const VERIFIED_DEPLOY_PROTOCOL = "amage.verified-deploy/2";
export const DEFAULT_VERIFIED_WAIT_SECONDS = 600;

const CAPABILITY_PATH = "/api/amage/verified_deploy";
// Authoritative installation state (contract §9), read with the token.
const INSTALLATIONS_PATH = "/api/amage/installations";
const CANDIDATE_FILE_EXTENSIONS = [".bend", ".ts", ".js", ".json"];
const CANDIDATE_SKIPPED_DIRECTORIES = new Set(["_generated", "node_modules"]);
const CANDIDATE_MAX_BYTES = 2 * 1024 * 1024;
// The supervisor only ever hands back `<member>/model.js`.
const GENERATED_FILE_PATTERN = /^[A-Za-z0-9_-]+\/model\.js$/;
const AUTHORITY_POLL_MS = 2_000;
const BUILD_TIMEOUT_MS = 10 * 60_000;
const SUPERVISOR_TIMEOUT_MS = 30_000;

const capability = looseObject({
  protocol: z.string(),
  required: z.boolean(),
  supervisorUrl: z.string().optional(),
  // The backend's trust set; informative for the CLI (the backend checks).
  verifierKeys: z
    .array(looseObject({ kid: z.string(), notAfter: z.string().optional() }))
    .optional(),
});
export type VerifiedDeployCapability = z.infer<typeof capability> & {
  legacyBackend: boolean;
};

const invariantResult = looseObject({
  id: z.string(),
  version: z.number(),
  status: z.string(),
  diagnostic: z.unknown().optional(),
});

// The install receipt (contract §3.1), as the supervisor returns it next to
// its token. The CLI only reads it for provenance; the token is what it sends.
const receipt = looseObject({
  installationId: z.string(),
  candidateId: z.string(),
  requestSha256: z.string(),
  receiptSha256: z.string(),
  policy: looseObject({
    id: z.string(),
    version: z.number(),
    sha256: z.string(),
  }),
  kid: z.string().optional(),
});

// What `/v1/build` and `/v1/candidate` answer. `status` and `deployAllowed`
// decide; the rest is provenance for the developer and for `--verified-json`.
const decision = looseObject({
  status: z.string(),
  candidateId: z.string().nullish(),
  deployAllowed: z.boolean().optional(),
  rollout: z.string().optional(),
  kind: z.string().optional(),
  diagnostic: z.unknown().optional(),
  files: z.array(z.string()).optional(),
  policy: looseObject({
    id: z.string(),
    version: z.number(),
    sha256: z.string(),
  }).optional(),
  invariants: z.array(invariantResult).optional(),
  requestSha256: z.string().optional(),
  receiptSha256: z.string().optional(),
  request: z.record(z.string(), z.unknown()).optional(),
  token: z.string().optional(),
  receipt: receipt.optional(),
  installationId: z.string().optional(),
  generated: z.record(z.string(), z.string()).optional(),
});
type Decision = z.infer<typeof decision>;

const installationStatus = looseObject({
  installation: looseObject({ state: z.string() }),
});

// What `/v1/report` answers; the supervisor only marks a candidate deployed
// after reading the backend, so "pending" is a state, not an error.
const reportResult = looseObject({
  confirmation: z.string().nullish(),
});
const CONFIRMED = new Set(["backend-confirmed", "backend-observed"]);

type VerificationEvent = {
  status: "passed" | "failed" | "blocked" | "unavailable";
  candidateId?: string | null | undefined;
  policy?: Decision["policy"];
  invariants?: Decision["invariants"];
  requestSha256?: string | undefined;
  receiptSha256?: string | undefined;
  installationId?: string | undefined;
  confirmation?: string | undefined;
  deploy: { status: "deployed" | "skipped" | "failed"; detail: string };
};

export type VerifiedPushOptions = {
  adminKey: string;
  url: string;
  deploymentName: string | null;
  deploymentType?: DeploymentType;
  dryRun: boolean;
  verbose: boolean;
  debug: boolean;
  typecheck: TypeCheckMode;
  codegen: boolean;
  generateCommonJSApi?: boolean;
  debugBundlePath?: string | undefined;
  writePushRequest?: string | undefined;
  debugNodeApis: boolean;
  codegenOnlyThisComponent?: string | undefined;
  message?: string | null;
  verifiedJson?: boolean | undefined;
  verifiedWait?: number | undefined;
};

export async function fetchVerifiedDeployCapability(
  ctx: Context,
  options: { url: string; adminKey: string },
): Promise<VerifiedDeployCapability> {
  const fetch = deploymentFetch(ctx, {
    deploymentUrl: options.url,
    adminKey: options.adminKey,
    // A backend older than C7 answers 404, which is an answer, not a retry.
    shouldRetry: (error, response) =>
      error !== null || (response !== null && response.status >= 500),
  });
  let body: unknown;
  try {
    const response = await fetch(CAPABILITY_PATH, { method: "GET" });
    body = await response.json();
  } catch (error: unknown) {
    if (error instanceof ThrowingFetchError && error.response.status === 404) {
      return {
        protocol: VERIFIED_DEPLOY_PROTOCOL,
        required: false,
        legacyBackend: true,
      };
    }
    // Fail closed: without an answer we can't tell whether this deployment
    // is protected, so nothing gets pushed.
    logFailure(
      `Could not read the verified-deploy capability at ${options.url}${CAPABILITY_PATH}; nothing was deployed.`,
    );
    return await logAndHandleFetchError(ctx, error);
  }
  const parsed = capability.safeParse(body);
  if (!parsed.success) {
    return await ctx.crash({
      exitCode: 1,
      errorType: "fatal",
      printedMessage: `Unexpected verified-deploy capability from ${options.url}${CAPABILITY_PATH}: ${parsed.error.message}`,
    });
  }
  if (
    parsed.data.required &&
    parsed.data.protocol !== VERIFIED_DEPLOY_PROTOCOL
  ) {
    return await ctx.crash({
      exitCode: 1,
      errorType: "fatal",
      printedMessage: `${options.url} requires verified deploys with protocol "${parsed.data.protocol}", but this CLI speaks "${VERIFIED_DEPLOY_PROTOCOL}". Upgrade the CLI, or upgrade the backend if it is the older of the two.`,
    });
  }
  return { ...parsed.data, legacyBackend: false };
}

// Every `.bend`/`.ts`/`.js`/`.json` file under the functions directory, keyed
// by POSIX path relative to it. Reads go through `ctx.fs` so `npx convex dev`
// watches them. The supervisor partitions them (policy, candidate, previous
// `model.js` output); the CLI doesn't decide what matters.
export async function collectCandidateFiles(
  ctx: Context,
  functionsDir: string,
): Promise<Record<string, string>> {
  const files: Record<string, string> = {};
  let totalBytes = 0;
  const walk = (dir: string) => {
    for (const entry of ctx.fs.listDir(dir).sort(consistentPathSort)) {
      const absPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        if (!CANDIDATE_SKIPPED_DIRECTORIES.has(entry.name)) {
          walk(absPath);
        }
        continue;
      }
      if (!entry.isFile() || !isCandidateFile(entry.name)) {
        continue;
      }
      const contents = ctx.fs.readUtf8File(absPath);
      totalBytes += Buffer.byteLength(contents, "utf8");
      files[path.relative(functionsDir, absPath).split(path.sep).join("/")] =
        contents;
    }
  };
  walk(functionsDir);
  if (totalBytes > CANDIDATE_MAX_BYTES) {
    return await ctx.crash({
      exitCode: 1,
      errorType: "invalid filesystem data",
      printedMessage: `${functionsDir} holds ${(totalBytes / 1024 / 1024).toFixed(2)} MiB of candidate sources (.bend, .ts, .js, .json); verified deploys accept at most ${CANDIDATE_MAX_BYTES / 1024 / 1024} MiB.`,
    });
  }
  return files;
}

function isCandidateFile(name: string): boolean {
  return (
    name !== "tsconfig.json" &&
    !name.endsWith(".d.ts") &&
    CANDIDATE_FILE_EXTENSIONS.includes(path.extname(name))
  );
}

type Supervisor = { url: string; credential: string };

type SupervisorResult =
  | { ok: true; body: unknown }
  | { ok: false; transient: boolean; message: string };

async function postSupervisor(
  supervisor: Supervisor,
  op: "build" | "candidate" | "report",
  body: Record<string, unknown>,
  timeoutMs: number,
): Promise<SupervisorResult> {
  const url = `${supervisor.url}/v1/${op}`;
  let response: Response;
  try {
    response = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${supervisor.credential}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(timeoutMs),
    });
  } catch (error: any) {
    return {
      ok: false,
      transient: true,
      message: `Verifier unavailable at ${url} (${error?.cause?.code ?? error?.cause?.message ?? error?.name ?? error}); nothing was deployed.`,
    };
  }
  if (response.status === 401 || response.status === 403) {
    return {
      ok: false,
      transient: false,
      message: `Verifier at ${supervisor.url} rejected the ${CODING_CREDENTIAL_ENV_VAR_NAME} credential (HTTP ${response.status}).`,
    };
  }
  if (!response.ok) {
    return {
      ok: false,
      transient: response.status >= 500,
      message: `Verifier at ${url} answered HTTP ${response.status}: ${(await response.text()).slice(0, 500)}`,
    };
  }
  try {
    return { ok: true, body: await response.json() };
  } catch {
    return {
      ok: false,
      transient: false,
      message: `Verifier at ${url} returned a non-JSON response.`,
    };
  }
}

async function supervisorFor(
  ctx: Context,
  cap: VerifiedDeployCapability,
): Promise<Supervisor> {
  const url = process.env[SUPERVISOR_URL_ENV_VAR_NAME] ?? cap.supervisorUrl;
  if (!url) {
    return await ctx.crash({
      exitCode: 1,
      errorType: "fatal",
      printedMessage: `This deployment requires verified deploys but doesn't announce a supervisor. Set ${SUPERVISOR_URL_ENV_VAR_NAME} to the supervisor's base URL (e.g. http://127.0.0.1:47000).`,
    });
  }
  const credential = process.env[CODING_CREDENTIAL_ENV_VAR_NAME];
  if (!credential) {
    return await ctx.crash({
      exitCode: 1,
      errorType: "fatal",
      printedMessage: `This deployment requires verified deploys. Set ${CODING_CREDENTIAL_ENV_VAR_NAME} to the coding-principal credential issued by the supervisor operator; candidates are submitted with it.`,
    });
  }
  return { url: url.replace(/\/+$/, ""), credential };
}

function formatPolicy(policy: Decision["policy"]): string {
  return policy ? `${policy.id}@${policy.version}` : "";
}

function describeDiagnostic(diagnostic: unknown): string {
  const raw =
    typeof diagnostic === "string"
      ? diagnostic
      : diagnostic !== null &&
          typeof diagnostic === "object" &&
          typeof (diagnostic as { raw?: unknown }).raw === "string"
        ? (diagnostic as { raw: string }).raw
        : JSON.stringify(diagnostic);
  return (raw ?? "").trim().split("\n").slice(0, 3).join("\n        ");
}

function logVerificationFailure(d: Decision) {
  const policy = formatPolicy(d.policy);
  if (d.kind === "policy_change_requires_policy_authority") {
    logFailure("Policy change requires policy authority.");
    for (const file of d.files ?? []) {
      logError(`    ${file}`);
    }
  } else {
    logFailure(
      d.status === "rejected"
        ? `Verification failed${policy ? ` (${policy})` : ""}${d.kind ? `: ${d.kind}` : ""}`
        : `Verifier answered "${d.status}" instead of authorizing the deploy${d.kind ? ` (${d.kind})` : ""}`,
    );
    for (const invariant of d.invariants ?? []) {
      const name = `${invariant.id}@${invariant.version}`;
      const diagnostic = describeDiagnostic(invariant.diagnostic);
      logError(
        invariant.status === "accepted"
          ? `    ${chalkStderr.green("✓")} ${name}`
          : `    ${chalkStderr.red("✗")} ${name}${diagnostic ? ` — ${diagnostic}` : ""}`,
      );
    }
    if (d.diagnostic !== undefined) {
      logError(`    ${describeDiagnostic(d.diagnostic)}`);
    }
  }
  logError("  Deploy blocked. Previous verified deployment remains active.");
}

function provenance(d: Decision) {
  const r = d.receipt;
  return {
    candidateId: r?.candidateId ?? d.candidateId,
    policy: r?.policy ?? d.policy,
    invariants: d.invariants,
    requestSha256: r?.requestSha256 ?? d.requestSha256,
    receiptSha256: r?.receiptSha256 ?? d.receiptSha256,
    installationId: r?.installationId ?? d.installationId,
  };
}

// Contract §9: after a lost `finish_push` answer the backend's installation
// state is the truth. `finished` means the push landed.
async function installationFinished(
  ctx: Context,
  url: string,
  token: string,
  installationId: string,
): Promise<boolean> {
  const fetch = deploymentFetch(ctx, {
    deploymentUrl: url,
    adminKey: token,
    shouldRetry: (error, response) =>
      error !== null || (response !== null && response.status >= 500),
  });
  try {
    const response = await fetch(`${INSTALLATIONS_PATH}/${installationId}`, {
      method: "GET",
    });
    const parsed = installationStatus.safeParse(await response.json());
    return parsed.success && parsed.data.installation.state === "finished";
  } catch (error: unknown) {
    logVerbose(
      `Could not read installation ${installationId}: ${String(error)}`,
    );
    return false;
  }
}

// Worth asking the backend: the answer never arrived, or the backend rejected
// the finish for a reason its status route settles (already finished).
function reconcilable(error: unknown): boolean {
  return (
    !(error instanceof ThrowingFetchError) ||
    error.response.status >= 500 ||
    error.serverErrorData?.code === "VerifiedDeployRejected"
  );
}

async function writeGeneratedFiles(
  ctx: Context,
  functionsDir: string,
  generated: Record<string, string>,
  current: Record<string, string>,
) {
  for (const [relPath, contents] of Object.entries(generated)) {
    if (!GENERATED_FILE_PATTERN.test(relPath)) {
      return await ctx.crash({
        exitCode: 1,
        errorType: "fatal",
        printedMessage: `Verifier returned a generated file outside <member>/model.js (${relPath}); refusing to write it.`,
      });
    }
    if (current[relPath] === contents) {
      continue;
    }
    const absPath = path.join(functionsDir, relPath);
    ctx.fs.mkdir(path.dirname(absPath), {
      allowExisting: true,
      recursive: true,
    });
    ctx.fs.writeUtf8File(absPath, contents);
  }
}

// The protected counterpart of the bundle → start_push → codegen → typecheck →
// wait_for_schema → finish_push sequence in `components.ts`. Initial codegen
// already ran before this is called.
export async function runVerifiedPush(
  ctx: Context,
  parentSpan: Span,
  rootComponent: ComponentDirectory,
  components: Map<string, ComponentDirectory>,
  options: VerifiedPushOptions,
  cap: VerifiedDeployCapability,
): Promise<{
  startPushResponse: StartPushResponse;
  finishPushResponse: FinishPushDiff;
}> {
  const refused = options.dryRun
    ? "dry runs are not available for protected deployments (the backend evaluates the schema against live data during start_push)"
    : options.writePushRequest !== undefined
      ? "--write-push-request is not available for protected deployments (the verifier builds the push request)"
      : options.debugBundlePath !== undefined
        ? "--debug-bundle-path is not available for protected deployments (the verifier bundles the functions)"
        : options.debugNodeApis
          ? "--debug-node-apis is not available for protected deployments (Node actions are not accepted)"
          : options.codegenOnlyThisComponent !== undefined
            ? "--component-dir is not available for protected deployments (components are not accepted)"
            : null;
  const emit = (event: VerificationEvent) => {
    if (options.verifiedJson) {
      logOutput(JSON.stringify({ event: "verification", ...event }));
    }
  };
  const skipped = (detail: string) => ({ status: "skipped" as const, detail });
  if (refused !== null) {
    emit({ status: "blocked", deploy: skipped(refused) });
    return await ctx.crash({
      exitCode: 1,
      errorType: "fatal",
      printedMessage: refused,
    });
  }

  const supervisor = await supervisorFor(ctx, cap);
  const ask = async (
    op: "build" | "candidate",
    body: Record<string, unknown>,
    timeoutMs: number,
  ): Promise<Decision> => {
    const result = await postSupervisor(supervisor, op, body, timeoutMs);
    if (!result.ok) {
      emit({
        status: result.transient ? "unavailable" : "blocked",
        deploy: skipped(result.message),
      });
      return await ctx.crash({
        exitCode: 1,
        errorType: result.transient ? "transient" : "fatal",
        printedMessage: result.message,
      });
    }
    logVerbose(`Verifier /v1/${op}: ${JSON.stringify(result.body)}`);
    const parsed = decision.safeParse(result.body);
    if (!parsed.success) {
      const message = `Verifier at ${supervisor.url} returned an unexpected /v1/${op} response: ${parsed.error.message}`;
      emit({ status: "blocked", deploy: skipped(message) });
      return await ctx.crash({
        exitCode: 1,
        errorType: "fatal",
        printedMessage: message,
      });
    }
    return parsed.data;
  };

  const files = await collectCandidateFiles(ctx, rootComponent.path);
  changeSpinner("Verifying invariants…");
  let current = await parentSpan.enterAsync("verifiedBuild", () =>
    ask("build", { files }, BUILD_TIMEOUT_MS),
  );
  const rollout =
    current.rollout ?? (current.status === "ready" ? "manual" : "auto");

  // Rollout `manual`: the candidate is verified but a deployment authority has
  // to authorize it. Poll; never authorize from here.
  if (current.status === "ready" && current.candidateId) {
    const candidateId = current.candidateId;
    const waitSeconds = Math.max(
      0,
      options.verifiedWait ?? DEFAULT_VERIFIED_WAIT_SECONDS,
    );
    const deadline = Date.now() + waitSeconds * 1000;
    while (current.status === "ready" && Date.now() < deadline) {
      changeSpinner(
        `Candidate ${candidateId} verified; waiting for deployment authority… (${Math.ceil((deadline - Date.now()) / 1000)}s left)`,
      );
      await new Promise((resolve) => setTimeout(resolve, AUTHORITY_POLL_MS));
      current = await ask("candidate", { candidateId }, SUPERVISOR_TIMEOUT_MS);
    }
    if (current.status === "ready") {
      const message = `Candidate ${candidateId} is verified and waiting for deployment authority; nothing was deployed. Run again with --verified-wait <seconds> to wait longer.`;
      emit({
        status: "passed",
        ...provenance(current),
        deploy: skipped(message),
      });
      return await ctx.crash({
        exitCode: 1,
        errorType: "invalid filesystem data",
        printedMessage: message,
      });
    }
  }

  const authorized =
    current.status === "authorized" &&
    current.deployAllowed === true &&
    current.request !== undefined &&
    current.token !== undefined &&
    current.installationId !== undefined;
  if (!authorized) {
    if (current.status === "authorized") {
      // Authorized but unusable: a supervisor bug, not something to retry.
      const message = `Verifier at ${supervisor.url} authorized candidate ${current.candidateId ?? "?"} without ${current.deployAllowed === true ? "the artifact (request, token, installationId)" : "deployAllowed"}; nothing was deployed.`;
      emit({
        status: "blocked",
        ...provenance(current),
        deploy: skipped(message),
      });
      return await ctx.crash({
        exitCode: 1,
        errorType: "fatal",
        printedMessage: message,
      });
    }
    logVerificationFailure(current);
    emit({
      status:
        current.kind === "policy_change_requires_policy_authority"
          ? "blocked"
          : "failed",
      ...provenance(current),
      deploy: skipped(
        `${current.status}${current.kind ? `: ${current.kind}` : ""}`,
      ),
    });
    // Like a type error: `npx convex dev` waits for the next edit.
    return await ctx.crash({
      exitCode: 1,
      errorType: "invalid filesystem data",
      printedMessage: null,
    });
  }
  const token = current.token!;
  const installationId = current.installationId!;
  const known = provenance(current);
  const candidateId = known.candidateId ?? "?";

  const checks = (current.invariants ?? [])
    .map((i) => `${i.id}@${i.version} ${chalkStderr.green("✓")}`)
    .join("  ");
  logFinishedStep(
    `Verification (${formatPolicy(known.policy)})${checks ? `: ${checks}` : " passed"}`,
  );
  logFinishedStep(
    `Artifact ${(known.requestSha256 ?? "").slice(0, 12)}… authorized by deployment authority (${rollout}); installation ${installationId.slice(0, 12)}`,
  );
  await writeGeneratedFiles(
    ctx,
    rootComponent.path,
    current.generated ?? {},
    files,
  );

  // Everything the supervisor returned goes to the backend as is, with the
  // receipt token where the admin key would go: it is the install credential,
  // and the operator's admin key (if any) never touches the push. Any other
  // root field would change the request digest the receipt signs.
  const request = startPushRequest.parse({
    ...current.request,
    adminKey: token,
  });
  const deploy = { ...options, adminKey: token };
  // A hint for the supervisor, which confirms with the backend itself.
  const report = async (
    outcome: "deployed" | "failed",
    detail: string,
  ): Promise<string> => {
    const result = await postSupervisor(
      supervisor,
      "report",
      { candidateId, installationId, outcome, detail },
      SUPERVISOR_TIMEOUT_MS,
    );
    if (!result.ok) {
      logWarning(
        chalkStderr.yellow(
          `Could not report the ${outcome} outcome of installation ${installationId} to the verifier: ${result.message}`,
        ),
      );
      return "pending_confirmation";
    }
    const parsed = reportResult.safeParse(result.body);
    return (
      (parsed.success && parsed.data.confirmation) || "pending_confirmation"
    );
  };

  showSpinner("Uploading functions to Convex...");
  const push = async () => {
    const startPushResponse = await parentSpan.enterAsync("startPush", (span) =>
      startPush(ctx, span, request, deploy),
    );
    if (options.verbose) {
      logMessage("startPush: " + JSON.stringify(startPushResponse, null, 2));
    }
    if (options.codegen) {
      changeSpinner("Generating TypeScript bindings...");
      await parentSpan.enterAsync("doFinalComponentCodegen", () =>
        withTmpDir(async (tmpDir) => {
          await doFinalComponentCodegen(
            ctx,
            tmpDir,
            rootComponent,
            rootComponent,
            startPushResponse,
            components,
            options,
          );
        }),
      );
    }
    changeSpinner("Running TypeScript...");
    await parentSpan.enterAsync("typeCheckFunctionsInMode", () =>
      typeCheckFunctionsInMode(ctx, options.typecheck, rootComponent.path),
    );
    await parentSpan.enterAsync("waitForSchema", (span) =>
      waitForSchema(ctx, span, startPushResponse, deploy),
    );
    const finishPushResponse = await parentSpan.enterAsync(
      "finishPush",
      async (span) => {
        // The backend ignores `message` on protected targets and writes its
        // own provenance line; the user's is passed through unchanged.
        const finish = { ...deploy, message: options.message ?? null };
        try {
          return await finishPush(ctx, span, startPushResponse, finish, true);
        } catch (error: unknown) {
          if (
            reconcilable(error) &&
            (await installationFinished(
              ctx,
              options.url,
              token,
              installationId,
            ))
          ) {
            const reason =
              error instanceof ThrowingFetchError
                ? (error.serverErrorData?.message ??
                  `HTTP ${error.response.status}`)
                : String(error);
            logFinishedStep(
              `Installation ${installationId.slice(0, 12)} reconciled from the backend: finish_push had already completed (${reason}).`,
            );
            // The finish diff was lost with the answer; index diffs still
            // print from the start_push response.
            return {
              authDiff: { added: [], removed: [] },
              definitionDiffs: {},
              componentDiffs: {},
            } satisfies FinishPushDiff;
          }
          return await handlePushConfigError(
            ctx,
            error,
            "Error: Unable to finish push to " + options.url,
            options.deploymentName,
            {
              adminKey: token,
              deploymentUrl: options.url,
              deploymentNotice: "",
            },
            options.deploymentType,
          );
        }
      },
    );
    return { startPushResponse, finishPushResponse };
  };
  let pushed: {
    startPushResponse: StartPushResponse;
    finishPushResponse: FinishPushDiff;
  };
  try {
    pushed = await push();
  } catch (error: unknown) {
    // Only reachable when `ctx.crash` throws (the dev loop); a one-off
    // command exits inside `crash` and the supervisor learns from the backend.
    await report("failed", String(error));
    emit({
      status: "passed",
      ...known,
      installationId,
      deploy: { status: "failed", detail: String(error) },
    });
    // eslint-disable-next-line no-restricted-syntax
    throw error;
  }
  const confirmation = await report(
    "deployed",
    `finish_push accepted by ${options.url}`,
  );
  if (CONFIRMED.has(confirmation)) {
    logFinishedStep(
      `Installation ${installationId.slice(0, 12)} confirmed by the backend`,
    );
  } else {
    logWarning(
      chalkStderr.yellow(
        `Installation ${installationId.slice(0, 12)} deployed; the supervisor could not yet confirm it with the backend (${confirmation}) and will reconcile later.`,
      ),
    );
  }
  emit({
    status: "passed",
    ...known,
    installationId,
    confirmation,
    deploy: { status: "deployed", detail: options.url },
  });
  return pushed;
}
