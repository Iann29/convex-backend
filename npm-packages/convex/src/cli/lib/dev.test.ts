import { afterEach, describe, expect, test, vi } from "vitest";
import { oneoffContext } from "../../bundler/context.js";

// `npx convex dev` against a protected deployment selected without an
// operator admin key (`adminKey: ""`): the push itself is exercised in
// verifiedDeploy.test.ts; here only what the missing key changes around it.
const mocks = vi.hoisted(() => ({
  watchLogs: vi.fn(() => new Promise<never>(() => {})),
  runPush: vi.fn(async () => {}),
}));
vi.mock("./logs.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./logs.js")>()),
  watchLogs: mocks.watchLogs,
}));
vi.mock("./components.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./components.js")>()),
  runPush: mocks.runPush,
}));
vi.mock("./config.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./config.js")>()),
  readProjectConfig: async () => ({
    projectConfig: { functions: "convex/" },
    configPath: "convex.json",
  }),
  getAuthKitConfig: async () => null,
}));

import { devAgainstDeployment } from "./dev.js";

class Exit extends Error {
  constructor(public code: number) {
    super(`exit ${code}`);
  }
}

async function ctxFor() {
  const ctx = await oneoffContext({});
  ctx.flushAndExit = async (code: number) => {
    throw new Exit(code);
  };
  return ctx;
}

const devOptions = {
  verbose: false,
  typecheck: "disable" as const,
  typecheckComponents: false,
  codegen: false,
  once: true,
  untilSuccess: false,
  tailLogs: "always" as const,
  traceEvents: false,
  debugNodeApis: false,
  liveComponentSources: false,
  pushAllModules: false,
};

async function exitCodeOf(promise: Promise<unknown>): Promise<number> {
  try {
    await promise;
  } catch (e) {
    expect(e).toBeInstanceOf(Exit);
    return (e as Exit).code;
  }
  throw new Error("expected dev to exit");
}

describe("dev without an admin key", () => {
  afterEach(() => {
    vi.clearAllMocks();
    vi.restoreAllMocks();
  });

  test("pushes once and doesn't tail logs, with a notice", async () => {
    const stderr: string[] = [];
    vi.spyOn(process.stderr, "write").mockImplementation((chunk: any) => {
      stderr.push(String(chunk));
      return true;
    });
    const code = await exitCodeOf(
      devAgainstDeployment(
        await ctxFor(),
        { url: "http://backend.test:3210", adminKey: "", deploymentName: null },
        devOptions,
      ),
    );
    expect(code).toBe(0);
    expect(mocks.runPush).toHaveBeenCalledTimes(1);
    expect(mocks.runPush.mock.calls[0][1 as never]).toMatchObject({
      adminKey: "",
      url: "http://backend.test:3210",
    });
    expect(mocks.watchLogs).not.toHaveBeenCalled();
    expect(stderr.join("")).toContain(
      "Function logs are not tailed: this protected deployment has no operator admin key in the environment (set CONVEX_SELF_HOSTED_ADMIN_KEY to tail logs).",
    );
  });

  test("tails logs as usual when an admin key is present", async () => {
    vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const code = await exitCodeOf(
      devAgainstDeployment(
        await ctxFor(),
        {
          url: "http://backend.test:3210",
          adminKey: "k",
          deploymentName: null,
        },
        devOptions,
      ),
    );
    expect(code).toBe(0);
    expect(mocks.watchLogs).toHaveBeenCalledTimes(1);
    expect(mocks.watchLogs.mock.calls[0][2 as never]).toBe("k");
  });

  test("--run <function> fails clearly before pushing anything", async () => {
    const stderr: string[] = [];
    vi.spyOn(process.stderr, "write").mockImplementation((chunk: any) => {
      stderr.push(String(chunk));
      return true;
    });
    const code = await exitCodeOf(
      devAgainstDeployment(
        await ctxFor(),
        { url: "http://backend.test:3210", adminKey: "", deploymentName: null },
        { ...devOptions, run: { kind: "function", name: "seed" } },
      ),
    );
    expect(code).toBe(1);
    expect(mocks.runPush).not.toHaveBeenCalled();
    expect(stderr.join("")).toContain("--run needs the deployment admin key");
  });
});
