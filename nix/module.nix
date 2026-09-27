# services.flower.instances.<name>: Flower nodes under systemd, as many as you like.
#
# The flake applies this file to itself, so packages default to the ones it
# builds. Each instance runs as flower-<name>.service with its own dynamic user
# and its data in /var/lib/private/flower-<name>. The operator token and the
# managed-key wrapping key never enter the Nix store: name files for them, or
# let the instance generate both on first start. `flower-<name>` wraps the SDK's
# command with the instance's URL and, for root, its operator token:
#
#   sudo flower-<name> deploy app.ts
self:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  inherit (lib)
    escapeShellArgs
    literalExpression
    mapAttrs'
    mapAttrsToList
    mkEnableOption
    mkIf
    mkOption
    nameValuePair
    optional
    optionals
    optionalString
    types
    ;
  packages = self.packages.${pkgs.stdenv.hostPlatform.system};
  instances = lib.filterAttrs (_: instance: instance.enable) config.services.flower.instances;

  portOf = listen: lib.toInt (lib.last (lib.splitString ":" listen));

  instanceOptions =
    { name, config, ... }:
    {
      options = {
        enable = mkEnableOption "this Flower node" // {
          default = true;
        };

        package = mkOption {
          type = types.package;
          default = packages.flower;
          defaultText = literalExpression "flower.packages.\${system}.flower";
          description = "The Flower server.";
        };

        cli = mkOption {
          type = types.package;
          default = packages.sdk;
          defaultText = literalExpression "flower.packages.\${system}.sdk";
          description = "The SDK, whose `flower` command bootstraps the cluster and backs `flower-${name}`.";
        };

        id = mkOption {
          type = types.ints.positive;
          default = 1;
          description = "Unique positive ID of this Raft node.";
        };

        listen = mkOption {
          type = types.str;
          default = "127.0.0.1:7101";
          example = "0.0.0.0:7101";
          description = ''
            HTTP/1.1 and HTTP/2 listen address, HOST:PORT. Cleartext (h2c) unless
            FLOWER_TLS_CERT_FILE and FLOWER_TLS_KEY_FILE are set in `environment`.
          '';
        };

        advertise = mkOption {
          type = types.nullOr types.str;
          default = null;
          example = "flower1.internal:7101";
          description = "Address other nodes reach this one at, without http://. Defaults to `listen`.";
        };

        adminTokenFile = mkOption {
          type = types.nullOr types.path;
          default = null;
          example = "/run/secrets/flower-admin-token";
          description = ''
            File holding the operator token (FLOWER_ADMIN_TOKEN), which protects
            deployment, cluster administration and, unless FLOWER_PEER_TOKEN is
            set through `environmentFile`, peer RPCs. Use a path outside the Nix
            store: it reaches the service through systemd's LoadCredential. When
            null, the instance generates one on first start, in `adminTokenPath`.
          '';
        };

        keyringFile = mkOption {
          type = types.nullOr types.path;
          default = null;
          example = "/run/secrets/flower-keyring";
          description = ''
            The 32-byte wrapping key that protects managed keys
            (FLOWER_KEYRING_FILE). When null, the instance generates one on first
            start, next to its data. Losing it makes every sealed key unreadable,
            so back it up apart from the data.
          '';
        };

        initialize = mkOption {
          type = types.bool;
          default = false;
          description = ''
            Bootstrap a one-member cluster (`flower init --members ID=ADDRESS`)
            the first time the service creates the data directory. Leave it off
            for a node that joins an existing cluster, and never enable it on a
            restored data directory's first start. Cleartext only.
          '';
        };

        environment = mkOption {
          type = types.attrsOf types.str;
          default = { };
          example = {
            RUST_LOG = "info";
            FLOWER_OTEL_ENABLED = "1";
          };
          description = "Extra environment: FLOWER_* tuning, TLS files, OpenTelemetry. RUST_LOG defaults to warn.";
        };

        environmentFile = mkOption {
          type = types.nullOr types.path;
          default = null;
          description = "systemd EnvironmentFile for secrets such as FLOWER_PEER_TOKEN or OTEL_EXPORTER_OTLP_HEADERS.";
        };

        extraArgs = mkOption {
          type = types.listOf types.str;
          default = [ ];
          description = "Further arguments to the server.";
        };

        openFirewall = mkOption {
          type = types.bool;
          default = false;
          description = "Open the listen port in the firewall.";
        };

        stateDirectory = mkOption {
          type = types.str;
          readOnly = true;
          default = "/var/lib/private/flower-${name}";
          description = "Where the instance keeps its data, and the token and keyring it generates.";
        };

        url = mkOption {
          type = types.str;
          readOnly = true;
          default =
            let
              port = toString (portOf config.listen);
            in
            "http://"
            + (
              if lib.hasPrefix "0.0.0.0:" config.listen then
                "127.0.0.1:${port}"
              else if lib.hasPrefix "[::]:" config.listen then
                "[::1]:${port}"
              else
                config.listen
            );
          description = "The instance's local URL.";
        };

        adminTokenPath = mkOption {
          type = types.str;
          readOnly = true;
          default =
            if config.adminTokenFile != null then
              toString config.adminTokenFile
            else
              "${config.stateDirectory}/admin-token";
          description = "The operator token's file, readable by root: for LoadCredential in units that deploy.";
        };
      };
    };

  unit =
    name: cfg:
    let
      # What other members, and `flower init`, reach this node at.
      address = if cfg.advertise != null then cfg.advertise else cfg.listen;

      # Before the server starts, so ExecStartPost's initialization finds what it needs.
      prepare = pkgs.writeShellScript "flower-${name}-prepare" ''
        set -eu
        generate() {
          if [ ! -s "$STATE_DIRECTORY/$1" ]; then
            $2 > "$STATE_DIRECTORY/$1.new"
            mv "$STATE_DIRECTORY/$1.new" "$STATE_DIRECTORY/$1"
          fi
        }
        ${optionalString (
          cfg.adminTokenFile == null
        ) ''generate admin-token "od -An -tx1 -N32 /dev/urandom"''}
        ${optionalString (cfg.keyringFile == null) ''generate keyring "head -c 32 /dev/urandom"''}
        ${optionalString cfg.initialize ''
          # Only a data directory this service creates gets initialized, once.
          if [ ! -e "$STATE_DIRECTORY/data" ]; then touch "$STATE_DIRECTORY/uninitialized"; fi
        ''}
      '';

      adminToken =
        if cfg.adminTokenFile != null then
          ''FLOWER_ADMIN_TOKEN="$(cat "$CREDENTIALS_DIRECTORY/admin-token")"''
        else
          ''FLOWER_ADMIN_TOKEN="$(tr -d ' \n' < "$STATE_DIRECTORY/admin-token")"'';

      start = pkgs.writeShellScript "flower-${name}-start" ''
        set -eu
        ${adminToken}
        export FLOWER_ADMIN_TOKEN
        export FLOWER_KEYRING_FILE=${
          if cfg.keyringFile != null then
            ''"$CREDENTIALS_DIRECTORY/keyring"''
          else
            ''"$STATE_DIRECTORY/keyring"''
        }
        exec ${lib.getExe cfg.package} ${
          escapeShellArgs (
            [
              "--id"
              (toString cfg.id)
              "--listen"
              cfg.listen
            ]
            ++ optionals (cfg.advertise != null) [
              "--advertise"
              cfg.advertise
            ]
            ++ cfg.extraArgs
          )
        } --data "$STATE_DIRECTORY/data"
      '';

      initialize = pkgs.writeShellScript "flower-${name}-initialize" ''
        set -eu
        [ -e "$STATE_DIRECTORY/uninitialized" ] || exit 0
        ${adminToken}
        export FLOWER_ADMIN_TOKEN
        for _ in $(seq 120); do
          ${lib.getExe pkgs.curl} -fsS ${cfg.url}/health >/dev/null 2>&1 && break
          sleep 0.5
        done
        ${lib.getExe cfg.cli} init --url ${cfg.url} --members ${toString cfg.id}=${address}
        rm "$STATE_DIRECTORY/uninitialized"
      '';
    in
    {
      description = "Flower database node ${name}";
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ];
      path = [ pkgs.coreutils ];
      environment = {
        RUST_LOG = "warn";
      }
      // cfg.environment;
      serviceConfig = {
        ExecStartPre = prepare;
        ExecStart = start;
        ExecStartPost = optional cfg.initialize initialize;
        Restart = "on-failure";
        RestartSec = 2;
        # Raft appends are fsynced; let a clean shutdown finish them.
        TimeoutStopSec = 30;

        DynamicUser = true;
        StateDirectory = "flower-${name}";
        StateDirectoryMode = "0700";
        UMask = "0077";
        LoadCredential =
          optional (cfg.adminTokenFile != null) "admin-token:${cfg.adminTokenFile}"
          ++ optional (cfg.keyringFile != null) "keyring:${cfg.keyringFile}";
        EnvironmentFile = optional (cfg.environmentFile != null) cfg.environmentFile;
        LimitNOFILE = 1048576;

        CapabilityBoundingSet = "";
        LockPersonality = true;
        # No MemoryDenyWriteExecute: Wasmtime compiles guest code to native code.
        NoNewPrivileges = true;
        PrivateDevices = true;
        PrivateTmp = true;
        ProtectClock = true;
        ProtectControlGroups = true;
        ProtectHome = true;
        ProtectHostname = true;
        ProtectKernelLogs = true;
        ProtectKernelModules = true;
        ProtectKernelTunables = true;
        ProtectProc = "invisible";
        ProtectSystem = "strict";
        RestrictAddressFamilies = [
          "AF_INET"
          "AF_INET6"
          "AF_UNIX"
        ];
        RestrictNamespaces = true;
        RestrictRealtime = true;
        SystemCallArchitectures = "native";
      };
    };

  # The SDK's command, aimed at the instance; root also gets its operator token.
  wrapper =
    name: cfg:
    pkgs.writeShellScriptBin "flower-${name}" ''
      export FLOWER_URL="''${FLOWER_URL:-${cfg.url}}"
      if [ -z "''${FLOWER_ADMIN_TOKEN:-}" ] && [ -r ${cfg.adminTokenPath} ]; then
        FLOWER_ADMIN_TOKEN="$(tr -d ' \n' < ${cfg.adminTokenPath})"
        export FLOWER_ADMIN_TOKEN
      fi
      exec ${lib.getExe cfg.cli} "$@"
    '';
in
{
  options.services.flower.instances = mkOption {
    type = types.attrsOf (types.submodule instanceOptions);
    default = { };
    example = literalExpression ''
      {
        main = {
          listen = "127.0.0.1:7101";
          initialize = true;
        };
        staging.listen = "127.0.0.1:7102";
      }
    '';
    description = "Flower nodes, each its own service (flower-<name>) with its own data.";
  };

  config = mkIf (instances != { }) {
    assertions =
      mapAttrsToList (name: cfg: {
        assertion = !(cfg.initialize && cfg.environment ? FLOWER_TLS_CERT_FILE);
        message = "services.flower.instances.${name}.initialize speaks cleartext; initialize a TLS node with `flower-${name} init` yourself.";
      }) instances
      ++ [
        {
          assertion =
            let
              ports = mapAttrsToList (_: cfg: portOf cfg.listen) instances;
            in
            lib.length ports == lib.length (lib.unique ports);
          message = "services.flower.instances: each instance needs its own port.";
        }
      ];

    networking.firewall.allowedTCPPorts = lib.concatLists (
      mapAttrsToList (_: cfg: optional cfg.openFirewall (portOf cfg.listen)) instances
    );

    environment.systemPackages = mapAttrsToList wrapper instances;

    systemd.services = mapAttrs' (name: cfg: nameValuePair "flower-${name}" (unit name cfg)) instances;
  };
}
