# The NixOS module end to end in a VM: two instances side by side, one with a
# generated operator token and one with a token file. Each is bootstrapped,
# gets an example application deployed through its `flower-<name>` command,
# keeps its data to itself, and survives a restart without initializing again.
# Run with `nix build .#checks.x86_64-linux.nixos -L`.
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
    services.flower.instances = {
      main = {
        listen = "127.0.0.1:7101";
        initialize = true;
      };
      staging = {
        listen = "127.0.0.1:7102";
        initialize = true;
        adminTokenFile = "/etc/flower-staging-token";
      };
    };
    environment.etc."flower-staging-token".text = "staging-operator-token\n";
  };

  testScript = ''
    for name, port in (("main", 7101), ("staging", 7102)):
        machine.wait_for_unit(f"flower-{name}.service")
        machine.wait_until_succeeds(f"curl -fsS http://127.0.0.1:{port}/health")

    token = machine.succeed("tr -d ' \\n' < /var/lib/private/flower-main/admin-token")
    assert len(token) == 64, token
    machine.wait_until_succeeds(
        f"curl -fsS -H 'Authorization: Bearer {token}' http://127.0.0.1:7101/raft/metrics | grep -q Leader"
    )
    machine.wait_until_succeeds(
        "curl -fsS -H 'Authorization: Bearer staging-operator-token' http://127.0.0.1:7102/raft/metrics | grep -q Leader"
    )
    machine.fail("test -e /var/lib/private/flower-staging/admin-token")

    for name in ("main", "staging"):
        state = f"/var/lib/private/flower-{name}"
        machine.succeed(f"test \"$(stat -c %a {state}/keyring)\" = 600")
        machine.succeed(f"test \"$(stat -c %s {state}/keyring)\" = 32")
        machine.fail(f"test -e {state}/uninitialized")
        machine.succeed(f"flower-{name} deploy ${app}/app.ts")

    # Each instance keeps its data to itself.
    machine.succeed("flower-main call order.create @${app}/args.json --request-id create-42")
    created = machine.succeed("flower-main query order.get '\"order-42\"'")
    assert '"total": 3700' in created, created
    machine.fail("flower-staging query order.get '\"order-42\"'")

    # Without root, no operator token: deploying is refused.
    machine.fail("su nobody -s /bin/sh -c 'flower-main deploy ${app}/app.ts'")

    keyring = machine.succeed("sha256sum /var/lib/private/flower-main/keyring")
    machine.succeed("systemctl restart flower-main")
    machine.wait_for_unit("flower-main.service")
    assert created == machine.wait_until_succeeds("flower-main query order.get '\"order-42\"'")
    assert keyring == machine.succeed("sha256sum /var/lib/private/flower-main/keyring")
    assert token == machine.succeed("tr -d ' \\n' < /var/lib/private/flower-main/admin-token")
    machine.fail("journalctl -u flower-main | grep -i 'already initialized'")
  '';
}
