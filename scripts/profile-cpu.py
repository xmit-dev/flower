#!/usr/bin/env python3
"""Summarize an exported xctrace Time Profiler table, without reading its TOC.

Only samples explicitly recorded as Running contribute on-CPU stack rankings.
Weights are statistical nanosecond weights, not exact CPU counters. Optional
Wasm symbol maps must belong to the sampled PID and matching guest artifact.
"""
import argparse
from collections import Counter
import importlib.util
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

MAX_XML_BYTES = 64 * 1024 * 1024
MAX_ELEMENTS = 1_000_000
MAX_ROWS = 100_000
MAX_STACK_FRAMES = 512
MAX_NAME_CHARS = 16_384
MAX_SYMBOL_BYTES = 4 * 1024 * 1024
MAX_MAP_BYTES = 16 * 1024 * 1024
MAX_MAP_RECORDS = 1024
MAX_MAP_FUNCTIONS = 250_000
MAX_WEIGHT = (1 << 63) - 1
FLOWER_FRAME = re.compile(r"^(?:flower::|<flower::)")
COPY_FRAME = re.compile(r"^(?:_platform_)?(?:memcpy|memmove)(?:\b|$)|^__?mem(?:cpy|move)(?:\b|$)")


def read_bounded(path, limit):
    with Path(path).open("rb") as file:
        result = file.read(limit + 1)
    if len(result) > limit:
        raise ValueError(f"input exceeds {limit} byte limit")
    return result


def load_symbols(sidecar_path, map_path):
    spec = importlib.util.spec_from_file_location("flower_wasm_profile", Path(__file__).with_name("profile-wasm.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sidecar = json.loads(read_bounded(sidecar_path, MAX_SYMBOL_BYTES))
    records = [json.loads(line) for line in read_bounded(map_path, MAX_MAP_BYTES).splitlines() if line.strip()]
    if len(records) > MAX_MAP_RECORDS or sum(len(record.get("functions", [])) for record in records) > MAX_MAP_FUNCTIONS:
        raise ValueError("native map exceeds record/function bounds")
    if not isinstance(sidecar.get("functions"), dict) or any(not isinstance(name, str) or len(name) > MAX_NAME_CHARS for name in sidecar["functions"].values()):
        raise ValueError("invalid guest function names")
    symbols = module.Symbols(sidecar, records)  # Checks guest identity, PID uniqueness and ambiguous ranges.
    pid = records[0]["pid"]
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        raise ValueError("native map has invalid PID")
    return symbols, pid


class References:
    def __init__(self, root):
        self.ids = {}
        for index, element in enumerate(root.iter(), 1):
            if index > MAX_ELEMENTS:
                raise ValueError("XML exceeds element limit")
            identifier = element.get("id")
            if identifier is not None:
                if identifier in self.ids:
                    raise ValueError("duplicate XML identifier")
                self.ids[identifier] = element

    def resolve(self, element):
        if element is None:
            return None
        tag, seen = element.tag, set()
        while "ref" in element.attrib:
            reference = element.get("ref")
            if reference in seen or len(seen) >= 16:
                raise ValueError("cyclic or excessive XML reference chain")
            seen.add(reference)
            element = self.ids.get(reference)
            if element is None:
                return None
            if element.tag != tag:
                raise ValueError("XML reference changes element type")
        return element


def demangle(names, execute=subprocess.run):
    raw = sorted({name for name in names if name.startswith(("_R", "_ZN"))})
    if not raw:
        return {}, "No Rust symbols required demangling"
    mapped = {}
    try:
        for start in range(0, len(raw), 256):
            batch = raw[start:start + 256]
            result = execute(["/usr/bin/xcrun", "llvm-cxxfilt"], input="\n".join(batch) + "\n", text=True, capture_output=True, timeout=10, check=True)
            lines = result.stdout.splitlines()
            if len(lines) != len(batch) or any(len(name) > MAX_NAME_CHARS for name in lines):
                raise ValueError("invalid demangler output")
            mapped.update(zip(batch, lines))
        return mapped, "xcrun llvm-cxxfilt"
    except (OSError, subprocess.SubprocessError, ValueError):
        return {}, "Unavailable; raw Rust symbols retained"


def parse_export(data):
    if len(data) > MAX_XML_BYTES:
        raise ValueError("XML exceeds byte limit")
    if re.search(br"<!\s*(?:DOCTYPE|ENTITY)\b", data, re.IGNORECASE):
        raise ValueError("DTD/entity declarations are not accepted")
    # xctrace emits UTF-8. Reject alternate encodings that could hide declaration
    # delimiters from the check above, and stop parsing at the element bound.
    data.decode("utf-8")
    if b"\x00" in data:
        raise ValueError("expected UTF-8 XML without NUL bytes")
    count = 0
    parser = ET.iterparse(io.BytesIO(data), events=("start", "end"))
    for event, _ in parser:
        if event == "start":
            count += 1
            if count > MAX_ELEMENTS:
                raise ValueError("XML exceeds element limit")
    root = parser.root
    # Do not accept a TOC/full export: it may contain inherited environment values.
    if root.tag != "trace-query-result" or len(root) != 1 or root[0].tag != "node":
        raise ValueError("expected a direct export of one time-profile table")
    table = root[0]
    schema = table.find("schema")
    if len(table.findall("schema")) != 1 or schema is None or schema.get("name") != "time-profile" or any(element.tag not in ("schema", "row") for element in table):
        raise ValueError("expected only the time-profile schema and rows")
    columns = {column.findtext("mnemonic"): column.findtext("engineering-type") for column in schema.findall("col")}
    if columns.get("thread-state") != "thread-state" or columns.get("weight") != "weight" or columns.get("stack") != "tagged-backtrace":
        raise ValueError("unexpected Time Profiler column types")
    if sum(element.tag == "row" for element in table) > MAX_ROWS:
        raise ValueError("XML exceeds sample row limit")
    return root, table


def analyze(data, symbols=None, symbol_pid=None, demangler=demangle, top=100):
    if not 1 <= top <= 1000:
        raise ValueError("top must be between 1 and 1000")
    root, table = parse_export(data)
    refs = References(root)
    raw_names = {element.get("name", "") for element in root.iter("frame") if "ref" not in element.attrib}
    if any(len(name) > MAX_NAME_CHARS for name in raw_names):
        raise ValueError("frame name exceeds limit")
    names, demangling = demangler(raw_names)
    states, selfs, inclusive, nearest, copy_parents, copy_nearest = (Counter() for _ in range(6))
    process_weights, issues = Counter(), Counter()
    running_weight = available_weight = partial_weight = missing_weight = unresolved_weight = 0
    rows = running_rows = mapped_frames = 0
    frame_cache = {}
    timestamps = []

    def frame_name(frame, pid):
        nonlocal mapped_frames
        key = (id(frame), pid)
        if key in frame_cache:
            return frame_cache[key]
        name = frame.get("name") or frame.get("addr") or "[unnamed frame]"
        is_unknown = bool(re.fullmatch(r"0x[0-9a-fA-F]+", name)) or name in ("???", "[unnamed frame]")
        if symbols is not None and pid == symbol_pid and is_unknown:
            try:
                match = symbols.resolve(int(frame.get("addr", name), 16))
            except ValueError:
                match = None
            if match:
                name, _ = match
                mapped_frames += 1
                is_unknown = False
        result = (names.get(name, name), is_unknown)
        frame_cache[key] = result
        return result

    for row in table.findall("row"):
        rows += 1
        weight_element = refs.resolve(row.find("weight"))
        try:
            text = weight_element.text if weight_element is not None else ""
            if not re.fullmatch(r"\d+", text or ""):
                raise ValueError()
            weight = int(text)
            if weight > MAX_WEIGHT:
                raise ValueError()
        except ValueError:
            raise ValueError("sample has missing or invalid nanosecond weight") from None
        state_element = refs.resolve(row.find("thread-state"))
        state = state_element.text if state_element is not None else "[missing state]"
        if not state or len(state) > 256:
            state = "[invalid state]"
        states[state] += weight
        if state != "Running":
            continue
        running_rows += 1
        running_weight += weight
        process = refs.resolve(row.find("process"))
        pid_element = refs.resolve(process.find("pid")) if process is not None else None
        pid = int(pid_element.text) if pid_element is not None and re.fullmatch(r"\d+", pid_element.text or "") else None
        process_weights[str(pid) if pid is not None else "unknown"] += weight
        if symbols is not None and pid != symbol_pid:
            issues["runningRowsWithoutMatchingMapPid"] += 1
        timestamp = refs.resolve(row.find("sample-time"))
        if timestamp is not None and re.fullmatch(r"\d+", timestamp.text or ""):
            timestamps.append(int(timestamp.text))
        stack = refs.resolve(row.find("tagged-backtrace"))
        if stack is None or not len(stack):
            missing_weight += weight
            issues["runningRowsWithoutStack"] += 1
            continue
        if len(stack) > MAX_STACK_FRAMES:
            raise ValueError("backtrace exceeds frame limit")
        frames = []
        unknown = False
        for element in stack:
            if element.tag != "frame":
                raise ValueError("unexpected non-frame backtrace entry")
            frame = refs.resolve(element)
            if frame is None:
                frames = None
                break
            name, unresolved = frame_name(frame, pid)
            frames.append(name)
            unknown |= unresolved
        if frames is None:
            partial_weight += weight
            issues["runningRowsWithUnresolvedFrameReferences"] += 1
            continue
        available_weight += weight
        if unknown:
            unresolved_weight += weight
        selfs[frames[0]] += weight
        inclusive.update({name: weight for name in set(frames)})
        owned = next((name for name in frames if FLOWER_FRAME.match(name)), "[outside Flower]")
        nearest[owned] += weight
        if COPY_FRAME.match(frames[0]):
            copy_parents.update({name: weight for name in set(frames[1:])})
            copy_nearest[owned] += weight

    def ranking(counter):
        return [{"frame": name, "sampledRunningMs": value / 1_000_000,
                 "percentOfAllRunningWeight": value * 100 / running_weight if running_weight else None}
                for name, value in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:top]]

    return {
        "schemaVersion": 1, "source": "xctrace time-profile table", "weightUnit": "nanoseconds",
        "rows": rows, "runningRows": running_rows, "recordedStateWeightMs": {key: value / 1_000_000 for key, value in sorted(states.items())},
        "sampledRunningMs": running_weight / 1_000_000,
        "runningWithCompleteExportedStackMs": available_weight / 1_000_000,
        "runningWithMissingStackMs": missing_weight / 1_000_000,
        "runningWithPartialExportedStackMs": partial_weight / 1_000_000,
        "runningWithUnresolvedSymbolsMs": unresolved_weight / 1_000_000,
        "stackCoverage": available_weight / running_weight if running_weight else None,
        "recordedRunningWeightMsByPid": {key: value / 1_000_000 for key, value in sorted(process_weights.items())},
        "firstRunningSampleNs": min(timestamps) if timestamps else None,
        "lastRunningSampleNs": max(timestamps) if timestamps else None,
        "demangling": demangling, "wasmSymbolMapPid": symbol_pid,
        "uniqueMappedGuestFrames": mapped_frames, "issues": dict(issues),
        "self": ranking(selfs), "inclusive": ranking(inclusive), "nearestFlower": ranking(nearest),
        "copyInclusiveParents": ranking(copy_parents), "copyNearestFlower": ranking(copy_nearest),
        "limits": {"xmlBytes": MAX_XML_BYTES, "xmlElements": MAX_ELEMENTS, "sampleRows": MAX_ROWS, "stackFrames": MAX_STACK_FRAMES, "rankingRows": top},
        "notes": [
            "Only the exact recorded state Running is included. Other or missing states are excluded and their weights remain visible; function names are never used to infer CPU state.",
            "Weights are statistical Time Profiler nanoseconds across sampled threads. They are not exact process CPU counters, wall duration, hardware cycles, or TypeScript source attribution.",
            "Percentages use all recorded Running weight, including missing/partial stacks. Complete means all exported frame references resolved; the profiler may still have truncated unwinding.",
            "Each inclusive function is counted once per sample, including recursion. Inclusive parents overlap and must not be summed. Self and nearest-Flower rankings partition only available exported stacks; only the top requested rows are displayed.",
            "Nearest Flower requires direct flower:: or <flower:: ownership; generic library wrappers mentioning Flower types are excluded from ownership.",
            "Wasm symbolization is restricted to the map's PID and matching guest hash; ambiguous reused addresses remain unresolved. Native symbols rely on the profiler's matching binary images. Raw traces and TOCs can contain environment values and are not shareable report artifacts.",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", type=Path, help="direct exported time-profile XML table")
    parser.add_argument("--symbols", type=Path, help="guest C-name sidecar")
    parser.add_argument("--native-map", type=Path, help="native map from exactly one sampled PID")
    parser.add_argument("--top", type=int, default=100)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if bool(args.symbols) != bool(args.native_map):
        parser.error("--symbols and --native-map must be provided together")
    if args.output and args.output.resolve() in {path.resolve() for path in (args.profile, args.symbols, args.native_map) if path is not None}:
        parser.error("output must not overwrite an input")
    try:
        symbols, pid = load_symbols(args.symbols, args.native_map) if args.symbols else (None, None)
        result = analyze(read_bounded(args.profile, MAX_XML_BYTES), symbols, pid, top=args.top)
        output = json.dumps(result, indent=2) + "\n"
        if args.output:
            args.output.write_text(output)
        else:
            sys.stdout.write(output)
    except (OSError, ValueError, KeyError, TypeError, ET.ParseError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
