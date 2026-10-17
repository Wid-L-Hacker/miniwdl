# GREMLIN HUNT -- temporary diagnostic test. FORK DEV BRANCH ONLY; NEVER UPSTREAM.
#
# Replicates tests/test_6workflowrun.py::test_call_try_call_cache (upstream main)
# faithfully -- same WDL shape, same call_cache cfg, same three-execution
# structure with the sentinel planted between executions 1 and 2 -- but loops
# the whole scenario for GREMLIN_N iterations in a small process pool, and, on
# ANY expectation mismatch, fails loudly with an attributable evidence dump:
# the failing execution's own workflow.log records (each miniwdl execution
# writes workflow.log into its own timestamped run directory, so records are
# attributable per-execution, unlike pytest's per-test captured log), the flaky
# job's stderr.txt/stdout.txt, and a host-side sentinel timeline.
#
# The flaky command's failure branch additionally dumps diagnostics to stderr
# (control-dir listing, sentinel existence/stat, /proc/mounts) before exit 42,
# so a caught failure shows what the job itself saw of its Directory input.
# Success/failure logic is otherwise identical to the original test.
#
# Evidence is printed in full (job log) and also emitted as ::error workflow
# commands (check-run annotations, retrievable via the public API even when
# job logs are gated).
#
# Knobs: GREMLIN_N (default 20), GREMLIN_WORKERS (default 3),
# GREMLIN_FORCE_FLAKE=1 (skip the sentinel plant in iteration 0; test-the-test).

import glob
import logging
import multiprocessing
import os
import shutil
import sys
import tempfile
import time
import traceback
import unittest
from concurrent.futures import ProcessPoolExecutor, as_completed

import WDL
import WDL.runtime
import WDL.runtime.config

HUNT_N = int(os.environ.get("GREMLIN_N", "20"))
HUNT_WORKERS = int(os.environ.get("GREMLIN_WORKERS", "3"))
FORCE_FLAKE = bool(os.environ.get("GREMLIN_FORCE_FLAKE"))

WDL_TEXT = """
version development

workflow gremlin_hunt {
    input {
        Directory flaky_control
    }

    call? stable
    call? flaky { input: control = flaky_control }

    output {
        String? stable_out = stable.out
        String? flaky_out = flaky.out
    }
}

task stable {
    command <<<
        echo run >> stable_counter.txt
        echo "stable-ran" > stable_out.txt
    >>>
    output {
        String out = read_string("stable_out.txt")
    }
}

task flaky {
    input {
        Directory control
    }
    command <<<
        echo run >> flaky_counter.txt
        if [ -f "~{control}/go" ]; then
            echo "flaky-recovered" > flaky_out.txt
        else
            echo "GREMLIN-DIAG control=~{control}" >&2
            ls -la "~{control}" >&2 || echo "GREMLIN-DIAG ls-control-failed" >&2
            if [ -e "~{control}/go" ]; then echo "GREMLIN-DIAG go: exists-but-not-a-regular-file" >&2; else echo "GREMLIN-DIAG go: absent" >&2; fi
            stat "~{control}" >&2 || echo "GREMLIN-DIAG stat-control-failed" >&2
            stat "~{control}/go" >&2 || echo "GREMLIN-DIAG stat-go-failed" >&2
            echo "GREMLIN-DIAG workdir:" >&2
            pwd >&2
            ls -la >&2 || echo "GREMLIN-DIAG ls-workdir-failed" >&2
            cat /proc/mounts >&2 || echo "GREMLIN-DIAG no-proc-mounts" >&2
            exit 42
        fi
    >>>
    output {
        String out = read_string("flaky_out.txt")
    }
}
"""

EXPECT = [
    # (expected outputs, expected stable executions, expected flaky executions)
    ({"stable_out": "stable-ran", "flaky_out": None}, 1, 1),
    ({"stable_out": "stable-ran", "flaky_out": "flaky-recovered"}, 0, 1),
    ({"stable_out": "stable-ran", "flaky_out": "flaky-recovered"}, 0, 0),
]


def _read(path, cap=20000):
    try:
        with open(path, errors="replace") as infile:
            txt = infile.read()
        if len(txt) > cap:
            txt = txt[:cap] + f"\n... [truncated at {cap} chars; full file at {path}]"
        return txt
    except Exception as exn:  # noqa: BLE001 -- evidence gathering must not itself fail
        return f"<unreadable: {exn}>"


def _evidence_for_execution(base, iteration, ex):
    """Assemble the attributable evidence dump for one mismatched execution."""
    parts = []
    parts.append(f"=== GREMLIN EVIDENCE iteration={iteration} execution={ex['execution']} ===")
    parts.append(f"base_dir: {base}")
    parts.append(f"expected_outputs: {ex['expected_outputs']}")
    parts.append(f"observed_outputs: {ex.get('outputs')}")
    parts.append(
        f"expected_counters: stable={ex['expected_stable']} flaky={ex['expected_flaky']}; "
        f"observed_counters: stable={ex.get('stable_executions')} flaky={ex.get('flaky_executions')}"
    )
    if ex.get("raised"):
        parts.append("execution raised instead of returning outputs:\n" + ex["raised"])
    parts.append("--- sentinel timeline (host side) ---")
    for entry in ex["timeline"]:
        parts.append(str(entry))
    rundir = ex.get("rundir")
    if rundir:
        parts.append(f"--- workflow.log ({rundir}) ---")
        logpath = os.path.join(rundir, "workflow.log")
        if os.path.exists(logpath):
            log_lines = _read(logpath, cap=200000).splitlines()
            interesting = [
                ln
                for ln in log_lines
                if (
                    "tolerated failure" in ln
                    or "call cache hit" in ln
                    or "cache entry invalid" in ln
                    or " ERROR " in ln
                    or " WARNING " in ln
                )
            ]
            parts.append(
                f"[workflow.log: {len(log_lines)} lines total; "
                f"{len(interesting)} WARNING/ERROR/tolerated lines follow]"
            )
            parts.extend(interesting[-120:])
            parts.append("[workflow.log tail follows]")
            parts.extend(log_lines[-30:])
        else:
            parts.append("<workflow.log missing>")
        parts.append(f"rundir entries: {sorted(os.listdir(rundir))}")
        for job in ("call-flaky", "call-stable"):
            for stream in ("stderr.txt", "stdout.txt"):
                hits = glob.glob(os.path.join(rundir, "**", job, stream), recursive=True)
                if not hits:
                    parts.append(f"--- {job}/{stream}: NOT PRESENT under rundir ---")
                for hit in hits:
                    parts.append(f"--- {job}/{stream} ({hit}) ---")
                    parts.append(_read(hit))
    return "\n".join(parts)


def _run_iteration(iteration):
    sys.setrecursionlimit(200)  # mirror the upstream test harness
    result = {"iteration": iteration, "ok": True, "evidence": None}
    base = tempfile.mkdtemp(prefix=f"miniwdl_gremlin_{iteration}_")
    cache_dir = os.path.join(base, "call_cache")
    control_dir = os.path.join(base, "flaky_control")
    os.makedirs(control_dir)
    timeline = []

    def sentinel_state(label):
        go = os.path.join(control_dir, "go")
        entry = {
            "label": label,
            "time": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "go_exists": os.path.exists(go),
            "control_listing": sorted(os.listdir(control_dir)),
        }
        if os.path.exists(go):
            st = os.stat(go)
            entry["go_stat"] = {"mtime": st.st_mtime, "size": st.st_size}
        timeline.append(entry)

    logger = logging.getLogger(f"gremlin.iteration{iteration}")
    cfg = WDL.runtime.config.Loader(logger, [])
    cfg.override({"call_cache": {"put": True, "get": True, "dir": cache_dir}})

    with tempfile.NamedTemporaryFile(dir=base, suffix=".wdl", delete=False) as outfile:
        outfile.write(WDL_TEXT.encode("utf-8"))
        wdlfn = outfile.name
    doc = WDL.load(wdlfn)
    inputs = WDL.values_from_json(
        {"flaky_control": control_dir},
        doc.workflow.available_inputs,
        doc.workflow.required_inputs,
    )

    def count_executions(rundir, filename):
        ans = 0
        for fn in glob.glob(os.path.join(rundir, "**", filename), recursive=True):
            with open(fn) as infile:
                ans += sum(1 for _ in infile)
        return ans

    for idx, (exp_outputs, exp_stable, exp_flaky) in enumerate(EXPECT, start=1):
        if idx == 2:
            sentinel_state("before plant (after execution 1)")
            if not (FORCE_FLAKE and iteration == 0):
                with open(os.path.join(control_dir, "go"), "w") as outfile:
                    outfile.write("go\n")
            sentinel_state("after plant (before execution 2)")
        ex = {
            "execution": idx,
            "expected_outputs": exp_outputs,
            "expected_stable": exp_stable,
            "expected_flaky": exp_flaky,
            "timeline": list(timeline),
        }
        try:
            rundir, outputs = WDL.runtime.run(
                cfg, doc.workflow, inputs, run_dir=base, _test_pickle=True
            )
            ex["rundir"] = rundir
            ex["outputs"] = WDL.values_to_json(outputs)
            ex["stable_executions"] = count_executions(rundir, "stable_counter.txt")
            ex["flaky_executions"] = count_executions(rundir, "flaky_counter.txt")
        except Exception:  # noqa: BLE001 -- any raise here is evidence, not a harness pass
            ex["raised"] = traceback.format_exc()
        if idx == 2:
            sentinel_state("after execution 2")
            ex["timeline"] = list(timeline)
        mismatch = (
            ex.get("outputs") != exp_outputs
            or ex.get("stable_executions") != exp_stable
            or ex.get("flaky_executions") != exp_flaky
            or "raised" in ex
        )
        if mismatch:
            result["ok"] = False
            result["evidence"] = _evidence_for_execution(base, iteration, ex)
            return result
    return result


def _emit_annotations(dump):
    """Emit the dump as ::error workflow commands -> check-run annotations."""
    escaped = dump.replace("%", "%25").replace("\r", "%0D")
    lines = escaped.split("\n")
    chunks = []
    cur = ""
    for ln in lines:
        piece = ln + "%0A"
        if len(cur) + len(piece) > 3500 and cur:
            chunks.append(cur)
            cur = ""
        cur += piece
    if cur:
        chunks.append(cur)
    total = len(chunks)
    for i, chunk in enumerate(chunks, start=1):
        print(f"::error title=GREMLIN EVIDENCE {i}/{total}::{chunk}", flush=True)


def _fs_stamp_probe() -> str:
    """Back-to-back file creates: how often does the second file's mtime_ns NOT
    exceed the first's? A nonzero rate means this filesystem stamps coarsely
    enough that a file created after a cache envelope can look 'not newer'."""
    d = tempfile.mkdtemp(prefix="gremlin_probe_")
    ties = 0
    trials = 200
    try:
        for _ in range(trials):
            a = os.path.join(d, "a")
            b = os.path.join(d, "b")
            with open(a, "w") as f:
                f.write("{}")
            with open(b, "w") as f:
                f.write("go\n")
            if os.stat(b).st_mtime_ns <= os.stat(a).st_mtime_ns:
                ties += 1
            os.remove(a)
            os.remove(b)
    finally:
        shutil.rmtree(d, ignore_errors=True)
    return f"fs stamp probe in {tempfile.gettempdir()}: ties={ties}/{trials}"


def _step_summary(line: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        try:
            with open(path, "a") as f:
                f.write(line + "\n")
        except Exception:
            pass


class TestGremlinHunt(unittest.TestCase):
    def test_gremlin_hunt(self):
        probe = _fs_stamp_probe()
        print(probe, flush=True)
        _step_summary(f"gremlin hunt: {probe}")
        ctx = multiprocessing.get_context("fork")
        failures = []
        completed = 0
        with ProcessPoolExecutor(max_workers=HUNT_WORKERS, mp_context=ctx) as pool:
            futs = {pool.submit(_run_iteration, i): i for i in range(HUNT_N)}
            try:
                for fut in as_completed(futs, timeout=2700):
                    i = futs[fut]
                    completed += 1
                    try:
                        res = fut.result()
                    except Exception:  # noqa: BLE001 -- worker crash is evidence too
                        failures.append(
                            {
                                "iteration": i,
                                "ok": False,
                                "evidence": "=== GREMLIN EVIDENCE (worker crash) "
                                f"iteration={i} ===\n" + traceback.format_exc(),
                            }
                        )
                        continue
                    if not res["ok"]:
                        failures.append(res)
            except TimeoutError:
                failures.append(
                    {
                        "iteration": -1,
                        "ok": False,
                        "evidence": "=== GREMLIN EVIDENCE (pool timeout) ===\n"
                        f"only {completed}/{HUNT_N} iterations completed within 2700s",
                    }
                )
        if failures:
            dump = probe + "\n\n" + "\n\n".join(
                f["evidence"] for f in failures if f.get("evidence")
            )
            print(dump, flush=True)
            _step_summary(f"gremlin hunt: CAUGHT {len(failures)}/{HUNT_N} (see annotations)")
            _emit_annotations(dump)
            self.fail(
                f"GREMLIN CAUGHT: {len(failures)}/{HUNT_N} iterations mismatched "
                "(full evidence printed above and emitted as check-run annotations)"
            )
        msg = f"GREMLIN HUNT CLEAN: {HUNT_N} iterations ({3 * HUNT_N} executions) passed"
        print(msg, flush=True)
        print(f"::notice title=GREMLIN HUNT::{msg}; {probe}", flush=True)
        _step_summary(f"gremlin hunt: CLEAN {HUNT_N} iterations; {probe}")


if __name__ == "__main__":
    unittest.main()
