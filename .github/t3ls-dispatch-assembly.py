"""One-time candidate-tree assembly. Never updates a branch or creates a release."""
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.request import Request, urlopen

BASE = "8af51f3a0ad4f53dc636c907a86cd8310b992b11"
TREE = "14477b90c0cc31f5763364fa7e83807d6527f2dc"
ROOT = Path.cwd()
SERVER = "apps/server/src/orchestration-v2/"
NATIVE = "native/t3-code-ls/"
PATHS = [
    NATIVE + "crates/core/src/lib.rs", NATIVE + "crates/core/src/dispatch.rs",
    NATIVE + "crates/core/src/bin/t3-code-ls-policy.rs",
    SERVER + "NativeDispatchFacts.ts", SERVER + "NativeDispatchPolicy.ts",
    NATIVE + "tools/check-dispatch.mjs", SERVER + "CommandPolicy.ts",
    SERVER + "NativeCommandPolicy.ts", SERVER + "Orchestrator.ts",
    NATIVE + "tools/check-lust.py", ".github/workflows/t3-code-ls.yml",
    NATIVE + "README.md",
]

def git(*args):
    return subprocess.check_output(["git", "--no-replace-objects", *args], text=True).strip()

def replace(path, old, new, count=1):
    file = ROOT / path
    text = file.read_text()
    if text.count(old) != count:
        raise RuntimeError(f"Unexpected source boundary in {path}: expected {count} instances of {old!r}")
    file.write_text(text.replace(old, new))

assert git("rev-parse", f"{BASE}^{{tree}}") == TREE
if "--publish" not in sys.argv:
    replace(SERVER + "CommandPolicy.ts", "export interface CommandPolicyV2Shape {\n", """export interface CommandPolicyV2Shape {
  readonly resolveMessageDispatchIntent: (
    ...args: Parameters<typeof resolveMessageDispatchIntent>
  ) => Effect.Effect<ReturnType<typeof resolveMessageDispatchIntent>>;
  readonly goalCommandStopsActiveRun: (
    ...args: Parameters<typeof goalCommandStopsActiveRun>
  ) => Effect.Effect<boolean>;
""")
    replace(SERVER + "CommandPolicy.ts", "  return {\n    ensureQueuedMessages,", """  return {
    resolveMessageDispatchIntent: Effect.fn("CommandPolicy.resolveMessageDispatchIntent")(
      (...args: Parameters<typeof resolveMessageDispatchIntent>) =>
        Effect.sync(() => resolveMessageDispatchIntent(...args)),
    ),
    goalCommandStopsActiveRun: Effect.fn("CommandPolicy.goalCommandStopsActiveRun")(
      (...args: Parameters<typeof goalCommandStopsActiveRun>) =>
        Effect.sync(() => goalCommandStopsActiveRun(...args)),
    ),
    ensureQueuedMessages,""")
    replace(SERVER + "NativeCommandPolicy.ts", 'import * as Effect from "effect/Effect";\n', 'import * as Effect from "effect/Effect";\nimport { makeNativeDispatchPolicy } from "./NativeDispatchPolicy.ts";\n')
    replace(SERVER + "NativeCommandPolicy.ts", "  return {\n    ensureQueuedMessages:", "  return {\n    ...makeNativeDispatchPolicy((args, signal) => execute(executable, args, signal)),\n    ensureQueuedMessages:")
    replace(SERVER + "Orchestrator.ts", "  resolveMessageDispatchIntent,\n", "")
    replace(SERVER + "Orchestrator.ts", "  goalCommandStopsActiveRun,\n", "")
    replace(SERVER + "Orchestrator.ts", "let dispatchMode = resolveMessageDispatchIntent(\n", "let dispatchMode = yield* commandPolicy.resolveMessageDispatchIntent(\n")
    replace(SERVER + "Orchestrator.ts", "goalCommandStopsActiveRun(goalCommand, goal?.status ?? null)", "(yield* commandPolicy.goalCommandStopsActiveRun(goalCommand, goal?.status ?? null))", 2)
    replace(NATIVE + "tools/check-lust.py", "    hashes = {str(path.relative_to(REPO)): sha256(path) for path in sorted(files)}", """    files += [ROOT / "tools/check-dispatch.mjs",
              REPO / "apps/server/src/orchestration-v2/NativeDispatchFacts.ts",
              REPO / "apps/server/src/orchestration-v2/NativeDispatchPolicy.ts",
              REPO / "apps/server/src/orchestration-v2/CommandPolicy.ts",
              REPO / "apps/server/src/orchestration-v2/Orchestrator.ts"]
    hashes = {str(path.relative_to(REPO)): sha256(path) for path in sorted(files)}""")
    replace(NATIVE + "tools/check-lust.py", '            receipt["policy_parity"] = parity.stdout.strip()\n            print(parity.stdout, end="")', '''            receipt["policy_parity"] = parity.stdout.strip()
            print(parity.stdout, end="")
            dispatch = run(["node", "tools/check-dispatch.mjs", str(executable)],
                           cwd=ROOT, env=env, receipt=receipt,
                           name="compare Lust message dispatch and goal policy to upstream")
            receipt["dispatch_parity"] = dispatch.stdout.strip()
            print(dispatch.stdout, end="")''')
    replace(NATIVE + "tools/check-lust.py", "all three differential oracles passed.", "all four differential oracles passed.")
    workflow = ".github/workflows/t3-code-ls.yml"
    replace(workflow, "      - apps/server/src/orchestration-v2/NativeCommandPolicy.ts\n", """      - apps/server/src/orchestration-v2/NativeCommandPolicy.ts
      - apps/server/src/orchestration-v2/NativeDispatchFacts.ts
      - apps/server/src/orchestration-v2/NativeDispatchPolicy.ts
      - apps/server/src/orchestration-v2/Orchestrator.ts
""", 2)
    replace(workflow, "git sparse-checkout set --cone native/t3-code-ls packages/contracts packages/client-runtime\n", "git sparse-checkout set --cone native/t3-code-ls packages/contracts packages/client-runtime apps/server/src/orchestration-v2\n", 2)
    replace(workflow, "      - name: Test nightly selection and complete source comparison\n", """      - name: Compare dispatch and goal policy with the actual upstream functions
        run: node --disable-warning=ExperimentalWarning tools/check-dispatch.mjs
      - name: Test nightly selection and complete source comparison
""")
    replace(workflow, "          sha256sum target/lust/t3-code-ls-policy\n", """          node --disable-warning=ExperimentalWarning tools/check-dispatch.mjs \\
            "$PWD/target/lust/t3-code-ls-policy" --service "$PWD/target/lust/dispatch-proof.json"
          sha256sum target/lust/t3-code-ls-policy
""")
    replace(workflow, "            native/t3-code-ls/target/lust/policy-adapter-proof.json\n", "            native/t3-code-ls/target/lust/policy-adapter-proof.json\n            native/t3-code-ls/target/lust/dispatch-proof.json\n")
    replace(NATIVE + "README.md", "server. The repository is still mixed-language, not an all-Lust application.\n", """server. The repository is still mixed-language, not an all-Lust application.

The same executable now owns message-delivery precedence (queue, steer, restart,
and immediate start) and goal-command stop decisions. The orchestrator awaits
these operations through `CommandPolicyV2`, including both goal-stop paths.
The temporary adapter still joins projection records and classifies maintenance
messages; it sends only bounded capability/status facts, not prompts or IDs.
The Lust resolver preserves absent versus explicit delivery intent, the requested
versus latest-active target, preparation gaps, missing sessions, and maintenance
commands. Its decisions are compared with the pinned upstream implementation
through the production fact adapter. This is not a port of the complete
orchestrator, provider lifecycle, persistence, or UI.
""")
    replace(NATIVE + "README.md", "node --disable-warning=ExperimentalWarning tools/check-policy.mjs\n", "node --disable-warning=ExperimentalWarning tools/check-policy.mjs\nnode --disable-warning=ExperimentalWarning tools/check-dispatch.mjs\n")
    replace(NATIVE + "README.md", 'node tools/check-policy-adapter.mjs "$PWD/target/lust/t3-code-ls-policy"\n', 'node tools/check-policy-adapter.mjs "$PWD/target/lust/t3-code-ls-policy"\nnode tools/check-dispatch.mjs "$PWD/target/lust/t3-code-ls-policy" --service\n')
    print("Applied bounded edits to the pinned source; main and all refs remain unchanged.")
    raise SystemExit(0)

assert os.environ["GITHUB_REPOSITORY"] == "glopyglerky/t3code"
assert os.environ["GITHUB_REF"] == "refs/heads/work/ls-dispatch-assembly-20261108"
allowed = set(PATHS) | {".github/t3ls-dispatch-assembly.py", ".github/workflows/t3ls-dispatch-assembly.yml"}
changed = set(git("diff", "--name-only", BASE).splitlines())
if not changed <= allowed:
    raise RuntimeError(f"Unexpected changed paths: {sorted(changed - allowed)}")
assert set(PATHS) <= changed
subprocess.run(["git", "diff", "--check", BASE, "--", *PATHS], check=True)
subprocess.run(["git", "diff", "--stat", BASE, "--", *PATHS], check=True)

def post(endpoint, payload):
    request = Request("https://api.github.com/repos/glopyglerky/t3code/git/" + endpoint,
        data=json.dumps(payload).encode(), method="POST", headers={
            "Authorization": "Bearer " + os.environ["GITHUB_TOKEN"],
            "Accept": "application/vnd.github+json", "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "t3ls-dispatch-assembly"})
    with urlopen(request, timeout=30) as response:
        return json.load(response)

entries = []
for path in PATHS:
    data = (ROOT / path).read_bytes()
    local_sha = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
    remote = post("blobs", {"encoding": "base64", "content": base64.b64encode(data).decode()})
    if remote["sha"] != local_sha:
        raise RuntimeError("Blob identity mismatch")
    mode = git("ls-files", "--stage", "--", path).split()[0]
    entries.append({"path": path, "mode": mode, "type": "blob", "sha": local_sha})
result = post("trees", {"base_tree": TREE, "tree": entries})
print(json.dumps({"candidate_tree": result["sha"], "base_commit": BASE, "entries": entries}))
print("Created content-addressed Git objects only. No ref, branch, release, or schedule was changed.")
