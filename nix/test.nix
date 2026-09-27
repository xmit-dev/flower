# The NixOS module end to end in a VM: bootstrap a node, deploy an example
# application with the packaged SDK, call it, and restart without losing data or
# initializing again. Run with `nix build .#checks.x86_64-linux.nixos -L`.
{
  pkgs,
  module,
  sdk,
  example,
  exampleArgs,
}:
let
  # The example, importing the packaged SDK instead of the checkout's sources.
  app = pkgs.runCommand "flower-example" { } ''
    mkdir -p $out/node_modules/@flower-js
    ln -s ${sdk}/lib/node_modules/@flower-js/sdk $out/node_modules/@flower-js/sdk
    sed 's|"\.\./sdk/index\.ts"|"@flower-js/sdk"|' ${example} > $out/app.ts
    cp ${exampleArgs} $out/args.json
  '';
in
pkgs.testers.runNixOSTest {
  name = "flower";

  nodes.machine = {
    imports = [ module ];
    services.flower = {
      enable = true;
      initialize = true;
      adminTokenFile = pkgs.writeText "flower-admin-token" "test-operator-token\n";
    };
    environment.systemPackages = [ sdk ];
    environment.variables.FLOWER_ADMIN_TOKEN = "test-operator-token";
  };

  testScript = ''
    machine.wait_for_unit("flower.service")
    machine.succeed("curl -fsS http://127.0.0.1:7101/health")
    machine.wait_until_succeeds(
        "curl -fsS -H 'Authorization: Bearer test-operator-token' http://127.0.0.1:7101/raft/metrics | grep -q Leader"
    )

    # Generated once, private to the service.
    machine.succeed("test \"$(stat -c %a /var/lib/private/flower/keyring)\" = 600")
    machine.succeed("test \"$(stat -c %s /var/lib/private/flower/keyring)\" = 32")
    machine.fail("test -e /var/lib/private/flower/uninitialized")

    machine.succeed("flower deploy ${app}/app.ts")
    machine.succeed("flower call order.create @${app}/args.json --request-id create-42")
    created = machine.succeed("flower query order.get '\"order-42\"'")
    assert '"total": 3700' in created, created

    keyring = machine.succeed("sha256sum /var/lib/private/flower/keyring")
    machine.succeed("systemctl restart flower")
    machine.wait_for_unit("flower.service")
    assert created == machine.wait_until_succeeds("flower query order.get '\"order-42\"'")
    assert keyring == machine.succeed("sha256sum /var/lib/private/flower/keyring")
    machine.fail("journalctl -u flower | grep -i 'already initialized'")
  '';
}
