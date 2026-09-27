#!/usr/bin/env python3
"""Package notices for the locked target dependency graph, without extra tools."""
import json
from pathlib import Path
import shutil
import subprocess
import sys

target, destination = sys.argv[1:]
output = Path(destination)
output.mkdir(parents=True, exist_ok=True)
metadata = json.loads(subprocess.check_output([
    "cargo", "metadata", "--locked", "--format-version", "1", "--filter-platform", target,
]))
packages = {package["id"]: package for package in metadata["packages"]}
nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
root = metadata["resolve"]["root"]
pending, selected = [root], set()
while pending:
    identity = pending.pop()
    if identity in selected:
        continue
    selected.add(identity)
    pending.extend(dependency["pkg"] for dependency in nodes[identity]["deps"]
                   if any(kind["kind"] != "dev" for kind in dependency["dep_kinds"]))

wasmtime = next(package for package in packages.values() if package["name"] == "wasmtime")
wasmtime_license = Path(wasmtime["manifest_path"]).parent / "LICENSE"
index = []
for identity in sorted(selected - {root}):
    package = packages[identity]
    directory = Path(package["manifest_path"]).parent
    notices = [path for path in directory.iterdir()
               if path.name.lower().startswith(("license", "licence", "notice", "copying"))]
    if package.get("license_file"):
        path = directory / package["license_file"]
        if path not in notices:
            notices.append(path)
    # Some workspace crates omit their repository's license when published.
    # Wasmtime's whole workspace shares this exact license + LLVM exception.
    if not notices and package["license"] == "Apache-2.0 WITH LLVM-exception" and package["name"].startswith(("wasmtime", "cranelift", "pulley")):
        notices = [wasmtime_license]
    if not notices and package["name"] in ("openraft", "openraft-macros"):
        notices = list(Path("release/licenses").glob("openraft-*"))
    if not notices:
        raise SystemExit(f"Missing license text for {package['name']} {package['version']}")
    dest = output / f"{package['name']}-{package['version']}"
    dest.mkdir(exist_ok=True)
    for notice in notices:
        if notice.is_dir():
            shutil.copytree(notice, dest / notice.name, dirs_exist_ok=True)
        else:
            shutil.copyfile(notice, dest / notice.name)
    # AWS-LC bundles native sources with additional third-party notices. Keep
    # their relative paths so nested LICENSE files never overwrite the root one.
    if package["name"] == "aws-lc-sys":
        native = directory / "aws-lc"
        for notice in native.rglob("*"):
            if notice.is_file() and notice.name.lower().startswith(("license", "licence", "notice", "copying")):
                target_notice = dest / "aws-lc" / notice.relative_to(native)
                target_notice.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(notice, target_notice)
    index.append({key: package.get(key) for key in ("name", "version", "license", "repository", "source")})
(output / "crates.json").write_text(json.dumps(index, indent=2) + "\n")
print(f"Packaged notices for {len(index)} target/build dependencies ({target})")
