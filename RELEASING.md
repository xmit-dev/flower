# Releases

`.github/workflows/release.yml` builds and publishes server binaries when a `vX.Y.Z` tag is pushed.
It also builds, tests, and automatically publishes new `@flower-js/sdk` versions
through npm trusted publishing. `Cargo.toml`, `Cargo.lock`, `package.json` and
`package-lock.json` must agree on the release version. A manual workflow run
builds and uploads testable artifacts; only version tags publish publicly.

The workflow first typechecks/tests the SDK and installs its actual tarball into
an isolated consumer project. It checks imports, declarations, the installed
CLI symlink and application bundling. The package contains compiled ESM,
TypeScript declarations and documentation; it ships no Rust source, tests,
server binary or native JavaScript engine. Its only production npm dependency
is esbuild, used by the bundle helper and CLI.

Four native jobs build with Rust 1.98.1 and run the Rust tests:

| Asset suffix | Build runner / runtime baseline |
| --- | --- |
| `x86_64-unknown-linux-gnu` | Ubuntu 22.04 x64; glibc 2.35+ |
| `aarch64-unknown-linux-gnu` | Ubuntu 22.04 arm64; glibc 2.35+ |
| `x86_64-apple-darwin` | macOS 15 Intel; macOS 15+ |
| `aarch64-apple-darwin` | macOS 15 Apple silicon; macOS 15+ |

Each `.tar.gz` contains `flower`, Flower’s MIT license, and third-party licensing
notices. The server embeds the vendored QuickJS-NG Wasm module; target machines
need neither a WASI runtime nor Node. Linux assets dynamically use system glibc;
these are not portable musl builds. macOS binaries are not Apple-notarized.
The `.sha256` files verify archive integrity. Windows binaries are not part of
this release matrix.

The GitHub release and npm jobs start only after every target succeeds. Stable
versions use npm's `latest` tag; versions containing a prerelease suffix use
`next` and a GitHub prerelease. Generated release notes identify changes since
the previous tag. Published npm versions are immutable: fix a released package
with a new version rather than reusing its tag.

## First npm publication and OIDC setup

Publish the first package manually from your npm account. The tested SDK tarball
is attached to each GitHub release and available in its Actions artifacts:

```sh
npm login
npm publish ./flower-js-sdk-0.1.0.tgz --access public
```

Use the tarball's actual version in that command. An npm maintainer must have
write access to the `@flower-js` scope. This creates `@flower-js/sdk`; configure
its trusted publisher in npm package settings with these exact values:

- Provider: GitHub Actions
- Organization: `xmit-dev`
- Repository: `flower`
- Workflow filename: `release.yml`
- Environment: `npm`
- Allowed action: direct **`npm publish`**

New publishers default to staged publication, so explicitly allow direct
publishing for this workflow. GitHub-hosted runners use Node 26.10.0 and npm's
OIDC exchange. Public-package provenance is generated automatically. See npm's
[trusted-publishing documentation](https://docs.npmjs.com/trusted-publishers/).

After that setup, pushing a new version tag publishes automatically. There is
no enable flag, publishing token secret, or separate CI approval switch. If the
package does not exist yet, the npm job reports that the first manual publish is
pending and completes without publishing; binary releases continue normally.
If the tagged version already exists, CI preserves it and skips publication.
Other registry/network errors and OIDC authentication failures fail the job.
Fix the configuration and rerun the failed job; do not move a published tag.

To inspect the registry decision without publishing:

```sh
node scripts/publish-sdk.mjs --check
# Also verify a downloaded tarball's identity/version:
node scripts/publish-sdk.mjs --check ./flower-js-sdk-0.1.0.tgz
```

## Prepare a release

```sh
npm ci
npm run check
npm run check:package
cargo test --locked --all-targets
```

Update the matching Cargo/npm versions and lockfiles, commit the reviewed
changes, then tag that commit and push the tag:

```sh
git tag v0.1.0
git push origin v0.1.0
```

Use the actual new version in those commands. `node scripts/check-release.mjs`
checks package identities/versions locally; the workflow additionally checks its
tag. `npm pack` automatically builds the SDK through `prepack`. For a dry run,
trigger **Release → Run workflow** on the intended branch in GitHub Actions;
no package or release is published from a branch run.
