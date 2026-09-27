{
  description = "A Raft-backed database of reactive TypeScript values";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    xmit = {
      url = "github:xmit-co/xmit";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
      xmit,
    }:
    let
      supportedSystems = [
        "aarch64-darwin"
        "x86_64-darwin"
        "aarch64-linux"
        "x86_64-linux"
      ];

      # Pin the current stable toolchain independently of the Nixpkgs release
      # so local and CI builds use the same compiler.
      rustVersion = "1.98.1";

      overlays = [ rust-overlay.overlays.default ];

      # The QuickJS guest is built with this exact WASI SDK release so that
      # vendor/quickjs-ng/build.py can reproduce the checked-in quickjs.wasm
      # byte for byte. Digests are the release's published asset digests.
      wasiSdkVersion = "34.0";
      wasiSdkArchives = {
        aarch64-darwin = {
          platform = "arm64-macos";
          sha256 = "9c59398106b417f8f14913380fdf0097a8cc0ff4af9eb3ce0065a859e88d49e9";
        };
        x86_64-darwin = {
          platform = "x86_64-macos";
          sha256 = "87d27fa8adc68dee59bfbf2e22a6d34ef717c34d6bf1d8af2a56fc929d9ce0eb";
        };
        aarch64-linux = {
          platform = "arm64-linux";
          sha256 = "f7e243dff54d60bcc576e94d6166b69f410f2500ae4a9ceef34315be10e77971";
        };
        x86_64-linux = {
          platform = "x86_64-linux";
          sha256 = "b761e3a0721dbae9c09a0059e5fdb2bf917d1b4a8a7b430fb3b5aafb0984b2c4";
        };
      };

      forAllSystems =
        f:
        nixpkgs.lib.genAttrs supportedSystems (
          system:
          f (
            import nixpkgs {
              inherit system;
              inherit overlays;
              config.allowDeprecatedx86_64Darwin = true;
            }
          )
        );

      mkWasiSdk =
        pkgs:
        let
          archive = wasiSdkArchives.${pkgs.stdenv.hostPlatform.system};
          name = "wasi-sdk-${wasiSdkVersion}-${archive.platform}";
        in
        pkgs.stdenvNoCC.mkDerivation {
          pname = "wasi-sdk";
          version = wasiSdkVersion;
          src = pkgs.fetchurl {
            url = "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${pkgs.lib.versions.major wasiSdkVersion}/${name}.tar.gz";
            inherit (archive) sha256;
          };
          # Prebuilt Linux binaries need their interpreter and libraries patched.
          nativeBuildInputs = pkgs.lib.optionals pkgs.stdenv.hostPlatform.isLinux [
            pkgs.autoPatchelfHook
          ];
          buildInputs = pkgs.lib.optionals pkgs.stdenv.hostPlatform.isLinux [
            pkgs.stdenv.cc.cc.lib
            pkgs.ncurses
            pkgs.zlib
          ];
          dontConfigure = true;
          dontBuild = true;
          # Keep the toolchain exactly as released, apart from ELF patching.
          dontStrip = true;
          installPhase = ''
            mkdir -p $out
            cp -R . $out
          '';
          meta.platforms = supportedSystems;
        };

      lib = nixpkgs.lib;
      version = (lib.importTOML ./Cargo.toml).package.version;

      # Only what each build reads, so edits to docs, benchmarks or the other
      # half of the repository don't rebuild it.
      serverSrc = lib.fileset.toSource {
        root = ./.;
        fileset = lib.fileset.unions [
          ./Cargo.toml
          ./Cargo.lock
          ./src
          # Workspace members: Cargo reads their manifests to resolve the lockfile.
          ./crates
          ./examples/goblin-pizza-rs
          ./vendor/openraft
          ./vendor/quickjs-ng/quickjs.wasm
          ./runtime/engine.js
        ];
      };

      sdkSrc = lib.fileset.toSource {
        root = ./.;
        fileset = lib.fileset.unions [
          ./package.json
          ./package-lock.json
          ./tsconfig.json
          ./tsconfig.build.json
          ./scripts/build-sdk.mjs
          ./sdk
          ./runtime/engine.js
          ./README.md
          ./LICENSE-MIT
        ];
      };

      commonMeta = pkgs: {
        homepage = "https://github.com/xmit-dev/flower";
        license = pkgs.lib.licenses.mit;
        platforms = supportedSystems;
      };

      mkFlower =
        pkgs:
        let
          rustToolchain = pkgs.rust-bin.stable.${rustVersion}.default;
        in
        (pkgs.rustPlatform.buildRustPackage.override {
          cargo = rustToolchain;
          rustc = rustToolchain;
        })
          {
            pname = "flower";
            inherit version;

            src = serverSrc;
            cargoLock.lockFile = ./Cargo.lock;
            cargoBuildFlags = [
              "--bin"
              "flower"
            ];
            # The Rust suite (cargo test, tests/*.rs) runs from a checkout; some of
            # its timing-sensitive cases would make the package flaky to build.
            doCheck = false;

            meta = commonMeta pkgs // {
              description = "A Raft-backed database of reactive TypeScript values";
              mainProgram = "flower";
            };
          };

      # @flower-js/sdk as npm would install it: dist/, package.json and its
      # runtime dependency (esbuild), under lib/node_modules/@flower-js/sdk.
      # bin/flower is the SDK's command (build, deploy, init, call…), not the server.
      mkSdk =
        pkgs:
        pkgs.buildNpmPackage {
          pname = "flower-sdk";
          inherit version;

          src = sdkSrc;
          nodejs = pkgs.nodejs_26;
          # Straight from package-lock.json's integrity hashes: no npmDepsHash to update.
          npmDeps = pkgs.importNpmLock {
            package = lib.importJSON ./package.json;
            packageLock = lib.importJSON ./package-lock.json;
          };
          inherit (pkgs.importNpmLock) npmConfigHook;

          meta = commonMeta pkgs // {
            description = "TypeScript SDK and command for the Flower reactive database";
            mainProgram = "flower";
          };
        };
    in
    {
      packages = forAllSystems (
        pkgs:
        let
          flower = mkFlower pkgs;
        in
        {
          inherit flower;
          default = flower;
          sdk = mkSdk pkgs;
          wasi-sdk = mkWasiSdk pkgs;
        }
      );

      apps = forAllSystems (
        pkgs:
        let
          packages = self.packages.${pkgs.stdenv.hostPlatform.system};
          server = {
            type = "app";
            program = "${packages.flower}/bin/flower";
            meta.description = "The Flower server";
          };
        in
        {
          default = server;
          flower = server;
          cli = {
            type = "app";
            program = "${packages.sdk}/bin/flower";
            meta.description = "The SDK's flower command: build, deploy, init, call, query, watch, keys";
          };
        }
      );

      # pkgs.flower and pkgs.flower-sdk, built with this flake's pinned Nixpkgs and Rust.
      overlays.default = final: _prev: {
        flower = self.packages.${final.stdenv.hostPlatform.system}.flower;
        flower-sdk = self.packages.${final.stdenv.hostPlatform.system}.sdk;
      };

      # services.flower: a Flower node under systemd. See nix/module.nix.
      nixosModules = {
        flower = import ./nix/module.nix self;
        default = self.nixosModules.flower;
      };

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          name = "flower-dev";

          packages = [
            # Rust guest modules (crates/, examples/goblin-pizza-rs) build for wasm32.
            (pkgs.rust-bin.stable.${rustVersion}.default.override {
              targets = [ "wasm32-unknown-unknown" ];
            })
            pkgs.nodejs_26
            pkgs.stdenv.cc
            xmit.packages.${pkgs.stdenv.hostPlatform.system}.default
          ];

          # Selected by vendor/quickjs-ng/build.py; ordinary Cargo builds don't need it.
          FLOWER_WASI_SDK = mkWasiSdk pkgs;
        };
      });

      checks = forAllSystems (
        pkgs:
        let
          packages = self.packages.${pkgs.stdenv.hostPlatform.system};
        in
        {
          inherit (packages) flower sdk;
        }
        // lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
          # A NixOS VM: the module bootstraps a node, the SDK deploys an example to it.
          nixos = import ./nix/test.nix {
            inherit pkgs;
            module = self.nixosModules.flower;
            inherit (packages) sdk;
            example = ./examples/orders.ts;
            exampleArgs = ./examples/orders.create.json;
          };
        }
      );

      formatter = forAllSystems (pkgs: pkgs.nixfmt);
    };
}
