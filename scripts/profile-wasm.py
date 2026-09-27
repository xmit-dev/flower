#!/usr/bin/env python3
"""Annotate macOS sample's anonymous guest frames using verified C-name sidecars.

Example: profile-wasm.py guest-symbols.json native-map.PID.jsonl sample.txt
Use a fresh map prefix and sample one live process after its bundles are loaded.
Conflicting address reuse is left unresolved instead of guessing a function.
"""
import argparse
import bisect
from collections import Counter
import json
from pathlib import Path
import re
import sys


class Symbols:
    def __init__(self, sidecar, records):
        self.names = sidecar["functions"]
        self.maps = []
        processes = set()
        for record in records:
            if record["guest_sha256"] != sidecar["guest_sha256"]:
                raise ValueError("native map and C symbols describe different guest artifacts")
            processes.add(record["pid"])
            functions = sorted(record["functions"], key=lambda function: function["offset"])
            self.maps.append((record["text_base"], record["text_length"], functions,
                              [function["offset"] for function in functions]))
        if len(processes) != 1:
            raise ValueError("provide a nonempty native map from exactly one process")

    def resolve(self, address):
        matches = set()
        for base, length, functions, offsets in self.maps:
            relative = address - base
            if not 0 <= relative < length:
                continue
            index = bisect.bisect_right(offsets, relative) - 1
            if index < 0:
                continue
            function = functions[index]
            displacement = relative - function["offset"]
            if displacement >= function["length"]:
                continue
            name = self.names.get(str(function["index"]), f"wasm[{function['index']}]")
            matches.add((name, displacement))
        return next(iter(matches)) if len(matches) == 1 else None


FRAME = re.compile(r"\?\?\?\s+\(in <unknown binary>\)\s+\[(0x[0-9a-fA-F]+)\]")


def annotate(text, symbols):
    counts = Counter()
    matched = 0
    summary = False
    lines = []
    for line in text.splitlines(keepends=True):
        if line.startswith("Sort by top of stack"):
            summary = True
        elif line.startswith("Binary Images:"):
            summary = False

        def replace(match):
            nonlocal matched
            resolved = symbols.resolve(int(match[1], 16))
            if resolved is None:
                return match[0]
            name, displacement = resolved
            matched += 1
            if summary:
                count = re.search(r"\]\s+(\d+)\s*$", line)
                if count:
                    counts[name] += int(count[1])
            return f"{name} (in Flower QuickJS Wasm) + {displacement} [{match[1]}]"

        lines.append(FRAME.sub(replace, line))
    return "".join(lines), counts, matched


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("symbols", type=Path)
    parser.add_argument("native_map", type=Path)
    parser.add_argument("sample", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        symbols = Symbols(json.loads(args.symbols.read_text()),
                          [json.loads(line) for line in args.native_map.read_text().splitlines() if line])
        output, counts, matched = annotate(args.sample.read_text(), symbols)
    except (ValueError, KeyError) as error:
        parser.error(str(error))
    if args.output:
        args.output.write_text(output)
    else:
        sys.stdout.write(output)
    print(f"Annotated {matched} guest frames. Sample counts below use the profiler's truncated top-of-stack summary:", file=sys.stderr)
    for name, count in counts.most_common(30):
        print(f"{count:8d}  {name}", file=sys.stderr)


if __name__ == "__main__":
    main()
