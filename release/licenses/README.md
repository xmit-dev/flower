Flower's own code is MIT licensed. Dependency licenses remain their authors'.

OpenRaft 0.9.25's published crates omit the root workspace licenses. These copies
come from the exact commit recorded in their `.cargo_vcs_info.json`:

- https://github.com/datafuselabs/openraft/blob/8815cdba2826f74e848acef361ad03f93bb1c3f8/LICENSE-MIT
- https://github.com/datafuselabs/openraft/blob/8815cdba2826f74e848acef361ad03f93bb1c3f8/LICENSE-APACHE

The release script collects other license and notice files from the locked Cargo
sources for each target. Wasmtime workspace crates with omitted license files
use the license shipped by the matching `wasmtime` crate. QuickJS-NG and its
guest support notices are separately included from `vendor/quickjs-ng`.
