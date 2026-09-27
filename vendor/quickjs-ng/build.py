#!/usr/bin/env python3
"""Reproduce Flower's embedded guest. Uses only Python stdlib and pinned SDK 34."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent
LOCK = ROOT / "SOURCES.json"
FUNCTIONS = {
    "_initialize": ([], []),
    "flower_init": ([], [0x7F]),
    "flower_alloc": ([0x7F], [0x7F]),
    "flower_free": ([0x7F], []),
    "flower_eval": ([0x7F] * 2, [0x7E]),
    "flower_compile": ([0x7F] * 2, [0x7E]),
    "flower_load": ([0x7F] * 2, [0x7E]),
    "flower_invoke": ([0x7F] * 5, [0x7E]),
    "flower_manifest": ([], [0x7E]),
    "flower_snapshot_prepare": ([], [0x7E]),
}


class Reader:
    def __init__(self, data):
        self.data = data
        self.offset = 0

    def take(self, size):
        end = self.offset + size
        if end > len(self.data):
            raise ValueError("truncated Wasm")
        result = self.data[self.offset:end]
        self.offset = end
        return result

    def byte(self):
        return self.take(1)[0]

    def uint(self):
        value = 0
        for shift in range(0, 35, 7):
            byte = self.byte()
            value |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return value
        raise ValueError("oversized Wasm integer")

    def string(self):
        return self.take(self.uint()).decode("utf-8")


def verify_wasm(data):
    reader = Reader(data)
    assert reader.take(8) == b"\x00asm\x01\x00\x00\x00", "not a Wasm core module"
    types, imports, functions, exports, globals_, memories = [], [], [], {}, [], []
    while reader.offset < len(data):
        tag = reader.byte()
        section = Reader(reader.take(reader.uint()))
        if tag == 1:
            for _ in range(section.uint()):
                assert section.byte() == 0x60, "unexpected Wasm type"
                params = list(section.take(section.uint()))
                results = list(section.take(section.uint()))
                types.append((params, results))
        elif tag == 2:
            for _ in range(section.uint()):
                module, name, kind = section.string(), section.string(), section.byte()
                assert kind == 0, "guest must import only functions"
                imports.append((module, name, section.uint()))
        elif tag == 3:
            functions = [section.uint() for _ in range(section.uint())]
        elif tag == 5:
            for _ in range(section.uint()):
                flags = section.uint()
                assert flags in (0, 1), "guest memory must be private nonshared wasm32"
                initial = section.uint()
                assert initial <= 128 * 1024 * 1024 // 65536, "guest initial memory exceeds budget"
                maximum = section.uint() if flags & 1 else None
                assert maximum is None or initial <= maximum, "invalid memory limits"
                memories.append((initial, maximum))
        elif tag == 6:
            for _ in range(section.uint()):
                valtype, mutable, operation = section.byte(), section.byte(), section.byte()
                assert valtype == 0x7F and operation == 0x41, "unexpected guest global"
                value = section.uint()  # This guest only has nonnegative i32 constants.
                assert section.byte() == 0x0B, "unexpected global initializer"
                globals_.append((mutable, value))
        elif tag == 7:
            for _ in range(section.uint()):
                name = section.string()
                assert name not in exports, "duplicate export"
                exports[name] = (section.byte(), section.uint())
        elif tag == 8:
            raise ValueError("guest must not run initialization implicitly")
    expected_imports = {
        ("flower", "host_call"): ([0x7F] * 3, [0x7E]),
        ("flower", "crypto_call"): ([0x7F] * 5, [0x7F]),
    }
    assert len(imports) == len(expected_imports), imports
    for module, name, index in imports:
        assert types[index] == expected_imports.pop((module, name)), "host callback signature"
    assert not expected_imports, "missing host imports"
    assert len(memories) == 1, "guest must define exactly one private memory"
    assert exports.keys() == FUNCTIONS.keys() | {"memory", "__stack_pointer"}, exports
    assert exports["memory"] == (2, 0), "memory export"
    kind, index = exports["__stack_pointer"]
    assert kind == 3 and globals_[index] == (1, 1024 * 1024), "C shadow stack layout"
    all_functions = [entry[2] for entry in imports] + functions
    for name, signature in FUNCTIONS.items():
        kind, index = exports[name]
        assert kind == 0 and types[all_functions[index]] == signature, name


def sections(data):
    reader = Reader(data)
    assert reader.take(8) == b"\x00asm\x01\x00\x00\x00"
    while reader.offset < len(data):
        tag = reader.byte()
        yield tag, reader.take(reader.uint())


def symbol_sidecar(named, artifact):
    # Metadata must not change even one executable/data byte or function index.
    assert [(tag, data) for tag, data in sections(named) if tag] == [
        (tag, data) for tag, data in sections(artifact) if tag
    ], "named diagnostic build differs from the checked-in guest"
    functions = {}
    for tag, data in sections(named):
        if tag:
            continue
        section = Reader(data)
        if section.string() != "name":
            continue
        while section.offset < len(section.data):
            kind = section.byte()
            subsection = Reader(section.take(section.uint()))
            if kind == 1:
                for _ in range(subsection.uint()):
                    index = subsection.uint()
                    functions[index] = subsection.string()
    assert functions, "diagnostic guest has no function names"
    return {"guest_sha256": hashlib.sha256(artifact).hexdigest(), "functions": functions}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    machine = {"aarch64": "arm64", "x86_64": "x86_64"}.get(platform.machine(), platform.machine())
    system = {"Darwin": "macos", "Linux": "linux"}.get(platform.system(), platform.system().lower())
    default_sdk = ROOT.parents[1] / ".tools" / f"wasi-sdk-34.0-{machine}-{system}"
    parser.add_argument("--sdk", type=Path, default=os.environ.get("FLOWER_WASI_SDK", default_sdk))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="verify byte-identical rebuild (default)")
    mode.add_argument("--write", action="store_true", help="replace guest and recorded artifact digest")
    mode.add_argument("--symbols", type=Path, help="write verified function-name sidecar without changing the guest")
    parser.add_argument("--verify-only", action="store_true", help="verify checked-in sources/artifact without a compiler")
    args = parser.parse_args()
    if args.symbols and args.verify_only:
        parser.error("--symbols requires a compiler and cannot use --verify-only")
    lock = json.loads(LOCK.read_text())
    for name, expected in lock["files"].items():
        actual = hashlib.sha256((ROOT / "upstream" / name).read_bytes()).hexdigest()
        if actual != expected:
            raise SystemExit(f"upstream source checksum mismatch: {name}")
    artifact = ROOT / "quickjs.wasm"
    if args.verify_only:
        output = artifact.read_bytes()
    else:
        compiler = args.sdk.resolve() / "bin" / "clang"
        version = subprocess.check_output([compiler, "--version"], text=True).splitlines()[0]
        if version != lock["toolchain"]["clang_version"]:
            raise SystemExit(f"unexpected compiler: {version}")
        with tempfile.TemporaryDirectory(prefix="flower-quickjs-") as temporary:
            destination = Path(temporary) / "quickjs.wasm"
            subprocess.run([
                str(compiler), "-O3", "-flto", "-msimd128", "-DNDEBUG", "-D_GNU_SOURCE", "-Iupstream",
                "-nostartfiles", "-Wl,--no-entry", "-Wl,--stack-first", "-Wl,-z,stack-size=1048576",
                "-Wl,--export=__stack_pointer", "-Wl,--export-memory",
                "-Wl,--strip-debug" if args.symbols else "-Wl,--strip-all", "-Wl,--lto-O3",
                "flower.c", "crypto.c", "engine.c", "upstream/dtoa.c", "upstream/libregexp.c",
                "upstream/libunicode.c", "-lm", "-o", str(destination),
            ], cwd=ROOT, check=True)
            output = destination.read_bytes()
    verify_wasm(output)
    if args.symbols:
        sidecar = symbol_sidecar(output, artifact.read_bytes())
        args.symbols.write_text(json.dumps(sidecar, indent=2) + "\n")
        print(f"Verified identical non-custom sections; wrote {len(sidecar['functions'])} function names to {args.symbols}")
        return
    digest = hashlib.sha256(output).hexdigest()
    if args.write and not args.verify_only:
        # Update the Rust embedded SHA-256 constant separately and run its tests.
        artifact.write_bytes(output)
        lock["artifact"] = {"sha256": digest, "bytes": len(output)}
        LOCK.write_text(json.dumps(lock, indent=2) + "\n")
    elif digest != lock["artifact"]["sha256"] or output != artifact.read_bytes():
        raise SystemExit(f"rebuild differs: {digest}; checked-in artifact is {lock['artifact']['sha256']}")
    print(f"QuickJS-NG {lock['upstream']['release']}: {len(output):,} bytes; SHA-256 {digest}")
    print("Verified pinned sources, two explicit flower imports, ten ABI functions, and 1 MiB guarded stack.")
    if not args.verify_only and not args.write:
        print("Rebuild is byte-for-byte identical to the checked-in artifact.")


if __name__ == "__main__":
    main()
