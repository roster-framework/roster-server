#!/usr/bin/env node
import { applyBundle, applyExitCode, BundleError, CLIENT_PROFILE, coreOrigin, planResult, prepareBundle, readPackageIdentity, RESULT_SCHEMA, type ImportResult } from "./bundle.js";

interface Arguments { command: "validate" | "import"; packagePath: string; bindingsPath: string; core: string; apply: boolean; expectedDigest: string | null }
function parseArguments(argv: string[]): Arguments {
  if (argv.length < 2 || argv[0] !== "bundle" || (argv[1] !== "validate" && argv[1] !== "import")) throw new BundleError("arguments", "usage_invalid");
  const command = argv[1]; const values = new Map<string, string>(); let apply = false;
  for (let index = 2; index < argv.length; index++) {
    const flag = argv[index]!;
    if (flag === "--apply") { if (apply || command !== "import") throw new BundleError("arguments", "usage_invalid"); apply = true; continue; }
    if (!["--package", "--bindings", "--core", "--expected-digest"].includes(flag) || values.has(flag) || !argv[index + 1] || argv[index + 1]!.startsWith("--")) throw new BundleError("arguments", "usage_invalid");
    values.set(flag, argv[++index]!);
  }
  if (!values.has("--package") || !values.has("--bindings") || !values.has("--core")) throw new BundleError("arguments", "usage_invalid");
  const expected = values.get("--expected-digest") ?? null;
  if (command === "validate" && (apply || expected !== null) || command === "import" && (!apply || !expected || !/^[0-9a-f]{64}$/u.test(expected))) throw new BundleError("arguments", "usage_invalid");
  return { command, packagePath: values.get("--package")!, bindingsPath: values.get("--bindings")!, core: values.get("--core")!, apply, expectedDigest: expected };
}
function failedResult(packageInfo: { name: string; version: string }, error: BundleError): ImportResult {
  return { schema: RESULT_SCHEMA, status: "failed", package: packageInfo, client_profile: CLIENT_PROFILE, digest: null,
    actions: [], applied: [], unresolved: [], failure: { stage: error.stage, code: error.code, ...(error.id ? { id: error.id } : {}) } };
}

export async function runBundleCli(argv: string[]): Promise<number> {
  const started = Date.now(); let packageInfo = { name: "", version: "" }; let result: ImportResult; let code = 2;
  try {
    const args = parseArguments(argv);
    packageInfo = await readPackageIdentity(args.packagePath);
    const totalLimit = args.command === "validate" ? 30_000 : 120_000;
    const deadline = started + totalLimit;
    coreOrigin(args.core);
    const plan = await prepareBundle(args.packagePath, args.bindingsPath, args.core, deadline);
    packageInfo = plan.package;
    if (args.command === "validate") {
      result = planResult(plan); code = 0;
    } else if (args.expectedDigest !== plan.digest) {
      throw new BundleError("digest", "digest_mismatch");
    } else {
      result = await applyBundle(plan, args.core, deadline);
      code = applyExitCode(result);
    }
  } catch (error) {
    const failure = error instanceof BundleError ? error : new BundleError("input", "input_invalid");
    result = failedResult(packageInfo, failure);
    code = failure.exitCode;
    if (failure.stage === "core" && failure.code === "core_rejected") code = 2;
  }
  try { process.stdout.write(JSON.stringify(result!) + "\n"); } catch { return 4; }
  return code;
}

void runBundleCli(process.argv.slice(2)).then((code) => { process.exitCode = code; }).catch(() => {
  process.stdout.write(JSON.stringify({ schema: RESULT_SCHEMA, status: "failed", package: { name: "", version: "" }, client_profile: CLIENT_PROFILE,
    digest: null, actions: [], applied: [], unresolved: [], failure: { stage: "input", code: "input_invalid" } }) + "\n");
  process.exitCode = 2;
});
