"""Fail on any broken invariant in the stress harness's results.

Reads the JSON that ``run.py`` writes and, optionally, what ``fuzz.py`` and
``sep_flow.py`` print, and exits 1 with one line per violation. Only invariants that
hold on any machine are checked: bytes, counts of winners, refusals and leftovers.
Throughput, latency and memory figures vary with the runner and are never compared.

    python stress/check.py results.json [--require name,...]
    python stress/check.py --fuzz fuzz.txt --sep-flow sep_flow.json

A scenario, or a part of one, that reports ``{"supported": false}`` is skipped, unless
it is named in ``--require``: a required scenario must be present and supported.
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

# Anywhere in a scenario's result, these keys must hold these values.
MUST_BE_ZERO = (
    "integrity_violations",
    "solo_integrity_violations",
    "race_integrity_violations",
    "committed_despite_mismatch",
    "match_completed_but_bytes_wrong",
    "completed_but_wrong_or_missing_bytes",
    "failed_but_committed",
    "failed_but_file_present",
    "temp_files_left",
    "bob_status_leaks",
    "bob_claim_wins",
)
MUST_BE_TRUE = (
    "integrity_ok",
    "gateway_alive_after",
    "hook_counts_exact",
    "matches_sha256_of_bytes_sent",
)
MUST_BE_FALSE = ("gateway_killed_for_memory",)
MUST_BE_EMPTY = ("invariant_violations",)

Problems = list[str]


def unsupported(node: Any) -> bool:
    return isinstance(node, dict) and node.get("supported") is False


def walk(node: Any, path: str) -> Iterator[tuple[str, dict[str, Any]]]:
    """Every dict under ``node`` with its dotted path, skipping unsupported parts."""
    if unsupported(node):
        return
    if isinstance(node, dict):
        yield path, node
        for key, value in node.items():
            yield from walk(value, f"{path}.{key}")
    elif isinstance(node, list):
        for i, value in enumerate(node):
            yield from walk(value, f"{path}[{i}]")


def check_generic(name: str, result: dict[str, Any], out: Problems) -> None:
    for path, node in walk(result, name):
        if "harness_error" in node:
            out.append(f"{path}: harness_error {node['harness_error']}")
        for key in MUST_BE_ZERO:
            if key in node and node[key] != 0:
                out.append(f"{path}.{key} = {node[key]!r}, expected 0")
        for key in MUST_BE_TRUE:
            if key in node and node[key] is not True:
                out.append(f"{path}.{key} = {node[key]!r}, expected true")
        for key in MUST_BE_FALSE:
            if key in node and node[key] is not False:
                out.append(f"{path}.{key} = {node[key]!r}, expected false")
        for key in MUST_BE_EMPTY:
            if key in node and node[key]:
                out.append(f"{path}.{key} = {node[key]!r}, expected empty")


def never_completed(path: str, outcomes: Any, out: Problems) -> None:
    if not isinstance(outcomes, dict):
        out.append(f"{path} missing")
    elif outcomes.get("completed"):
        out.append(f"{path}: {outcomes['completed']} completed, expected none")


def one_winner_each(path: str, by_count: Any, out: Problems) -> None:
    """``by_count`` maps a number of winners to how many records or tickets had it."""
    if not isinstance(by_count, dict) or not by_count:
        out.append(f"{path} missing or empty")
    elif set(by_count) != {"1"}:
        out.append(f"{path} = {by_count!r}, expected exactly one winner each")


def header_bomb(r: dict[str, Any], out: Problems) -> None:
    if r.get("outcome") == "completed":
        out.append("header_bomb.outcome = 'completed', expected a refusal")


def header_bomb_under_load(r: dict[str, Any], out: Problems) -> None:
    never_completed("header_bomb_under_load.bomb_outcomes", r.get("bomb_outcomes"), out)


def epilogue_valid(r: dict[str, Any], out: Problems) -> None:
    outcomes = r.get("outcomes")
    if not isinstance(outcomes, dict) or set(outcomes) != {"completed"}:
        out.append(f"epilogue_valid.outcomes = {outcomes!r}, expected all completed")


def burst_over_cap(r: dict[str, Any], out: Problems) -> None:
    peak, cap = r.get("backend_peak_concurrency"), r.get("cap")
    if not isinstance(peak, int) or not isinstance(cap, int):
        out.append(f"burst_over_cap: peak {peak!r} or cap {cap!r} missing")
    elif peak > cap:
        out.append(f"burst_over_cap.backend_peak_concurrency = {peak}, over the cap of {cap}")


def policy_edges(r: dict[str, Any], out: Problems) -> None:
    widen = r.get("widen_accept_issue")
    if widen is None or widen == "issued":
        out.append(f"policy_edges.widen_accept_issue = {widen!r}, expected a refusal")
    ttl = r.get("ttl_zero")
    if ttl is None or str(ttl).startswith("issued"):
        out.append(f"policy_edges.ttl_zero = {ttl!r}, expected a refusal")
    if r.get("exe_into_image_destination") == "completed":
        out.append("policy_edges.exe_into_image_destination = 'completed'")


def chaos(r: dict[str, Any], out: Problems) -> None:
    kinds = r.get("outcomes_by_kind") or {}
    for kind in ("oversize", "garbage", "junk_header"):
        never_completed(f"chaos.outcomes_by_kind.{kind}", kinds.get(kind), out)


def digest_integrity(r: dict[str, Any], out: Problems) -> None:
    kinds = r.get("outcomes_by_kind") or {}
    for kind in ("wrong_digest", "size_over", "size_under"):
        never_completed(f"digest_integrity.outcomes_by_kind.{kind}", kinds.get(kind), out)


def claim_race(r: dict[str, Any], out: Problems) -> None:
    for store in ("memory", "sqlite"):
        part = r.get(store)
        if unsupported(part):
            continue
        if not isinstance(part, dict):
            out.append(f"claim_race.{store} missing")
            continue
        by_count = part.get("records_by_winner_count")
        one_winner_each(f"claim_race.{store}.records_by_winner_count", by_count, out)
        if isinstance(by_count, dict) and part.get("total_wins") != sum(by_count.values()):
            out.append(f"claim_race.{store}.total_wins = {part.get('total_wins')!r}")


def two_replicas(name: str) -> Callable[[dict[str, Any], Problems], None]:
    def check(r: dict[str, Any], out: Problems) -> None:
        for key in ("race_tickets_by_winner_count", "claim_records_by_winner_count"):
            one_winner_each(f"{name}.{key}", r.get(key), out)
        for key in ("status_on_a", "status_on_b"):
            wrong = (r.get(key) or {}).get("wrong")
            if wrong != 0:
                out.append(f"{name}.{key}.wrong = {wrong!r}, expected 0")

    return check


def error_details(r: dict[str, Any], out: Problems) -> None:
    for case, entry in r.items():
        if isinstance(entry, dict) and "http" in entry and 200 <= entry["http"] < 300:
            out.append(f"error_details.{case}.http = {entry['http']}, expected a refusal")


def digest_format(r: dict[str, Any], out: Problems) -> None:
    if r.get("outcome") != "completed":
        out.append(f"digest_format.outcome = {r.get('outcome')!r}, expected completed")
    if "matches_sha256_of_bytes_sent" not in r:
        out.append("digest_format.matches_sha256_of_bytes_sent missing")


def filename_hygiene(r: dict[str, Any], out: Problems) -> None:
    for case, entry in r.items():
        if not isinstance(entry, dict) or "outcome" not in entry:
            continue
        if entry.get("has_u202e") is not False:
            out.append(f"filename_hygiene.{case}.has_u202e = {entry.get('has_u202e')!r}")
        if entry.get("is_nfc") is not True:
            out.append(f"filename_hygiene.{case}.is_nfc = {entry.get('is_nfc')!r}")
        size = entry.get("utf8_bytes")
        if not isinstance(size, int) or size > 255:
            out.append(f"filename_hygiene.{case}.utf8_bytes = {size!r}, expected <= 255")


def sink_mixed_failures(r: dict[str, Any], out: Problems) -> None:
    kinds = r.get("outcomes_by_kind") or {}
    for kind in ("oversize", "wrong_digest", "vanish"):
        never_completed(f"sink_mixed_failures.outcomes_by_kind.{kind}", kinds.get(kind), out)


def raw_uploads(r: dict[str, Any], out: Problems) -> None:
    kinds: dict[str, Any] = r.get("outcomes_by_kind") or {}
    for kind, outcomes in kinds.items():
        if kind != "honest":
            never_completed(f"raw_uploads.outcomes_by_kind.{kind}", outcomes, out)
    completed = (kinds.get("honest") or {}).get("completed", 0)
    if r.get("honest_names_ok") != completed:
        out.append(
            f"raw_uploads.honest_names_ok = {r.get('honest_names_ok')!r}, "
            f"expected {completed} (every completed honest upload)"
        )


SPECIFIC: dict[str, Callable[[dict[str, Any], Problems], None]] = {
    "header_bomb": header_bomb,
    "header_bomb_under_load": header_bomb_under_load,
    "epilogue_valid": epilogue_valid,
    "burst_over_cap": burst_over_cap,
    "policy_edges": policy_edges,
    "chaos": chaos,
    "digest_integrity": digest_integrity,
    "claim_race": claim_race,
    "redis_two_replicas": two_replicas("redis_two_replicas"),
    "sqlite_two_processes": two_replicas("sqlite_two_processes"),
    "error_details": error_details,
    "digest_format": digest_format,
    "filename_hygiene": filename_hygiene,
    "sink_mixed_failures": sink_mixed_failures,
    "raw_uploads": raw_uploads,
}


def check_run(path: Path, required: list[str], out: Problems, notes: list[str]) -> None:
    data = json.loads(path.read_text())
    scenarios = data.get("scenarios")
    if not isinstance(scenarios, dict) or not scenarios:
        out.append(f"{path}: no scenarios")
        return
    for name in required:
        if name not in scenarios:
            out.append(f"{name}: required but not in {path}")
        elif unsupported(scenarios[name]):
            out.append(f"{name}: required but reports supported: false")
    for name, result in scenarios.items():
        if not isinstance(result, dict):
            out.append(f"{name}: result is not an object")
            continue
        if unsupported(result):
            notes.append(f"skipped {name} (unsupported)")
            continue
        before = len(out)
        check_generic(name, result, out)
        if name in SPECIFIC and "harness_error" not in result:
            SPECIFIC[name](result, out)
        notes.append(f"{'FAIL' if len(out) > before else 'ok'} {name}")


def check_fuzz(path: Path, out: Problems) -> None:
    text = path.read_text().strip()
    try:
        report = ast.literal_eval(text.splitlines()[-1]) if text else None
    except (ValueError, SyntaxError):
        report = None
    if not isinstance(report, dict):
        out.append(f"fuzz: no result dict in {path}")
        return
    if not report.get("bodies"):
        out.append(f"fuzz.bodies = {report.get('bodies')!r}, expected some")
    for key in ("failed", "completed_with_wrong_bytes"):
        if report.get(key) != 0:
            out.append(f"fuzz.{key} = {report.get(key)!r}, expected 0 ({report.get('by_outcome')})")


def last_json_object(text: str) -> Any:
    """The report ``sep_flow.py`` prints, even if a child process wrote lines before it."""
    decoder = json.JSONDecoder()
    found: Any = None
    for at in [0] + [i + 1 for i, ch in enumerate(text) if ch == "\n"]:
        if text.startswith("{", at):
            try:
                found = decoder.raw_decode(text, at)[0]
            except ValueError:
                continue
    return found


def check_sep_flow(path: Path, out: Problems) -> None:
    report = last_json_object(path.read_text())
    if not isinstance(report, dict) or "scenarios" not in report:
        out.append(f"sep_flow: no report in {path}")
        return
    scenarios: dict[str, Any] = report["scenarios"]
    for name in ("good", "mismatch", "oversize", "double", "other_owner"):
        entry = scenarios.get(name)
        if not isinstance(entry, dict):
            out.append(f"sep_flow.{name} missing")
        elif entry.get("failed") != 0 or entry.get("passed") != entry.get("flows"):
            errors = entry.get("errors", [])
            out.append(
                f"sep_flow.{name}: {entry.get('passed')}/{entry.get('flows')} passed, "
                f"{entry.get('failed')} failed {errors}"
            )
    backend = report.get("backend") or {}
    commits = backend.get("commits")
    expected = backend.get("expected_commits")
    matching = backend.get("commits_matching_sent_sha256")
    if not (commits == expected == matching) or not isinstance(commits, int):
        out.append(
            f"sep_flow.backend: {commits!r} commits, {expected!r} expected, "
            f"{matching!r} matching the bytes sent"
        )
    if report.get("all_passed") is not True:
        out.append(f"sep_flow.all_passed = {report.get('all_passed')!r}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("run", nargs="?", type=Path, help="JSON written by run.py")
    parser.add_argument("--require", default="", help="scenarios that must be present")
    parser.add_argument("--fuzz", type=Path, help="what fuzz.py printed")
    parser.add_argument("--sep-flow", type=Path, help="what sep_flow.py printed")
    args = parser.parse_args()
    if not (args.run or args.fuzz or args.sep_flow):
        parser.error("give a run.py JSON, --fuzz or --sep-flow")
    required = [n for n in args.require.split(",") if n]
    if required and not args.run:
        parser.error("--require needs a run.py JSON")

    problems: Problems = []
    notes: list[str] = []
    if args.run:
        check_run(args.run, required, problems, notes)
    if args.fuzz:
        check_fuzz(args.fuzz, problems)
        notes.append(f"{'FAIL' if any(p.startswith('fuzz') for p in problems) else 'ok'} fuzz")
    if args.sep_flow:
        check_sep_flow(args.sep_flow, problems)
        failed = any(p.startswith("sep_flow") for p in problems)
        notes.append(f"{'FAIL' if failed else 'ok'} sep_flow")
    for note in notes:
        print(note)
    if problems:
        print(f"\n{len(problems)} invariant violation(s):", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1
    print("\nall invariants hold")
    return 0


if __name__ == "__main__":
    sys.exit(main())
