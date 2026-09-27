# services.flower: one Flower node under systemd.
#
# The flake applies this file to itself, so the package defaults to the server
# it builds. The node keeps its data, and unless keyringFile says otherwise its
# managed-key wrapping key, in /var/lib/flower. The operator token never enters
# the Nix store: systemd hands adminTokenFile to the service as a credential.
self:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.flower;
  inherit (lib)
    escapeShellArgs
    literalExpression
    mkEnableOption
    mkIf
    mkOption
    optional
    optionals
    types
    ;
  packages = self.packages.${pkgs.stdenv.hostPlatform.system};

  port = lib.toInt (lib.last (lib.splitString ":" cfg.listen));
  # What other members, and `flower init`, reach this node at.
  address = if cfg.advertise != null then cfg.advertise else cfg.listen;
  # A wildcard listener is reachable on loopback.
  localUrl =
    "http://"
    + (
      if lib.hasPrefix "0.0.0.0:" cfg.listen then
        "127.0.0.1:${toString port}"
      else if lib.hasPrefix "[::]:" cfg.listen then
        "[::1]:${toString port}"
      else
        cfg.listen
    );

  start = pkgs.writeShellScript "flower-start" ''
    set -eu
    FLOWER_ADMIN_TOKEN="$(cat "$CREDENTIALS_DIRECTORY/admin-token")"
    export FLOWER_ADMIN_TOKEN
    ${
      if cfg.keyringFile != null then
        ''export FLOWER_KEYRING_FILE="$CREDENTIALS_DIRECTORY/keyring"''
      else
        ''
          if [ ! -s "$STATE_DIRECTORY/keyring" ]; then
            head -c 32 /dev/urandom > "$STATE_DIRECTORY/keyring.new"
            mv "$STATE_DIRECTORY/keyring.new" "$STATE_DIRECTORY/keyring"
          fi
          export FLOWER_KEYRING_FILE="$STATE_DIRECTORY/keyring"
        ''
    }
    ${lib.optionalString cfg.initialize ''
      # Only a data directory this service creates gets initialized, once.
      if [ ! -e "$STATE_DIRECTORY/data" ]; then touch "$STATE_DIRECTORY/uninitialized"; fi
    ''}
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

  initialize = pkgs.writeShellScript "flower-initialize" ''
    set -eu
    [ -e "$STATE_DIRECTORY/uninitialized" ] || exit 0
    FLOWER_ADMIN_TOKEN="$(cat "$CREDENTIALS_DIRECTORY/admin-token")"
    export FLOWER_ADMIN_TOKEN
    for _ in $(seq 120); do
      ${lib.getExe pkgs.curl} -fsS ${localUrl}/health >/dev/null 2>&1 && break
      sleep 0.5
    done
    ${lib.getExe cfg.cli} init --url ${localUrl} --members ${toString cfg.id}=${address}
    rm "$STATE_DIRECTORY/uninitialized"
  '';
in
{
  options.services.flower = {
    enable = mkEnableOption "a Flower database node";

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
      description = "The SDK, whose `flower` command bootstraps the cluster when `initialize` is set.";
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
      type = types.path;
      example = "/run/secrets/flower-admin-token";
      description = ''
        File holding the operator token (FLOWER_ADMIN_TOKEN), which protects
        deployment, cluster administration and, unless FLOWER_PEER_TOKEN is
        set through `environmentFile`, peer RPCs. Use a path outside the Nix
        store; it is passed to the service with systemd's LoadCredential.
      '';
    };

    keyringFile = mkOption {
      type = types.nullOr types.path;
      default = null;
      example = "/run/secrets/flower-keyring";
      description = ''
        The 32-byte wrapping key that protects managed keys
        (FLOWER_KEYRING_FILE). When null, one is generated in
        /var/lib/flower/keyring on first start. Losing it makes every sealed
        key unreadable, so back it up apart from the data.
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
  };

  config = mkIf cfg.enable {
    assertions = [
      {
        assertion = !(cfg.initialize && cfg.environment ? FLOWER_TLS_CERT_FILE);
        message = "services.flower.initialize speaks cleartext; initialize a TLS node with `flower init` yourself.";
      }
    ];

    networking.firewall.allowedTCPPorts = optional cfg.openFirewall port;

    systemd.services.flower = {
      description = "Flower database node";
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ];
      environment = {
        RUST_LOG = "warn";
      }
      // cfg.environment;
      serviceConfig = {
        ExecStart = start;
        ExecStartPost = optional cfg.initialize initialize;
        Restart = "on-failure";
        RestartSec = 2;
        # Raft appends are fsynced; let a clean shutdown finish them.
        TimeoutStopSec = 30;

        DynamicUser = true;
        StateDirectory = "flower";
        StateDirectoryMode = "0700";
        UMask = "0077";
        LoadCredential = [
          "admin-token:${cfg.adminTokenFile}"
        ]
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
  };
}
