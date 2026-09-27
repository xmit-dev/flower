# TLS and peer/operator credentials

The native listener supports HTTP/1.1 and HTTP/2. With no TLS settings it serves
cleartext HTTP/1.1 and prior-knowledge h2c. Set all three mounted PEM files on
**every node** to enable native TLS:

```sh
export FLOWER_TLS_CERT_FILE=/run/flower/tls/node-chain.pem
export FLOWER_TLS_KEY_FILE=/run/flower/tls/node-key.pem
export FLOWER_TLS_CA_FILE=/run/flower/tls/peer-roots.pem
export FLOWER_ADMIN_TOKEN='<operator credential>'
export FLOWER_PEER_TOKEN='<separate cluster peer credential>'
flower --id 1 --listen 0.0.0.0:7101 --advertise db1.example.net:7101 --data ./node-1
```

The certificate file contains the leaf followed by intermediate certificates.
Its DNS/IP subject alternative name must cover the advertised hostname/IP and
the name clients use. The key must match the leaf; PKCS#8, PKCS#1 and SEC1 PEM
keys supported by Rustls can be loaded. The CA file contains the trusted peer
roots. Protect private-key and credential files independently of the database.
The listener negotiates `h2` or `http/1.1` through ALPN. TLS 1.2/1.3 use Rustls
with the same AWS-LC provider already used by Flower's native cryptography.

All internal clients—Raft, fresh-read fences, forwarding, partition routing and
migration, and cross-group transactions—use HTTPS when TLS is configured. They
trust **only** the configured CA roots, verify certificate validity and hostname,
and preserve peer node/group identity and runtime-compatibility checks. There
is no insecure-verification switch, HTTP downgrade, redirect or proxy fallback.
The listener does not require client certificates: peer requests authenticate
with their separate bearer credential. This is server-authenticated TLS plus
peer-token authentication, not mutual TLS.

`FLOWER_TLS_HANDSHAKE_TIMEOUT_MS` defaults to 10000. A silent or invalid handshake
closes without creating an HTTP application request. TLS handshake/transport
buffers are outside application preparation-memory accounting. HTTP body,
header, stream and graceful-shutdown settings continue to apply after TLS.
Certificate files and CA roots load once at startup; replacing a file does not
hot-reload active connections.

## Separate authority

`FLOWER_ADMIN_TOKEN` authorizes deployment, managed-key operations, retention,
partition administration, resource metrics, and Raft initialization/membership/
metrics. `FLOWER_PEER_TOKEN` authorizes internal replication, snapshot transfer,
read fences, forwarding, partition and transaction RPCs. A configured peer token
is rejected by operator endpoints; an operator token is rejected by internal
peer endpoints. Internal peers remain fully trusted: they can replicate state
and forward already authorized operator work. Compromising a peer credential is
a cluster compromise, not a sandbox escape prevented by route separation.

If `FLOWER_PEER_TOKEN` is absent, it defaults to the operator token for local
experiments. Set a distinct value to obtain credential separation. Neither
credential substitutes for the application's authorization hook, declared with
`define({ auth })`. Keep privileged endpoints network-restricted even when TLS
is enabled.

Peer tokens are cluster-wide and currently accept one value at a time. There is
no seamless dual-token rotation: use a coordinated maintenance window. Operator
tokens can be rotated node by node independently when peer tokens are separate.

## Clients

Use `https://` endpoint URLs. The default Fetch client and CLI use their host's
trust configuration; Node can receive a private CA through `NODE_EXTRA_CA_CERTS`
**before process startup**. Browsers require a certificate chain trusted by the
browser. The optional Node HTTP/2 adapter accepts explicit trust roots:

```ts
import { readFileSync } from "node:fs";
import { FlowerClient } from "@flower-js/sdk/client";
import { createHttp2Transport } from "@flower-js/sdk/http2";

const transport = createHttp2Transport({
  ca: readFileSync("./peer-roots.pem"),
  requestTimeoutMs: 10_000,
});
try {
  const db = new FlowerClient("https://db1.example.net:7101", { fetch: transport.fetch });
  console.log(await db.query("pizza.board"));
} finally {
  await transport.close();
}
```

`ca` accepts a nonempty PEM string, Uint8Array, or an array of either. Byte inputs
are copied when the transport is created. Explicit roots replace Node's default
roots for that transport; omission preserves Node's configured defaults. TLS
verification remains mandatory, and there is no hostname override. One HTTP/2
session is pooled per origin. Request deadlines, cancellation, SSE backpressure,
connection accounting and uncertain-write retry rules are unchanged. `FlowerAdmin`
accepts the same `fetch` option, so operator calls can share the transport.

Raft/group address registries remain `host:port` without a URL scheme. All peers
must agree on TLS mode; mixed cleartext/TLS groups are unsupported. Enabling TLS
on an existing cleartext group therefore needs a coordinated transport cutover,
not the ordinary compatible-binary rolling upgrade. For certificate rotation,
first distribute a CA bundle trusting old and new roots and restart nodes one
at a time; then rotate certificates one node at a time; finally remove old roots
and restart. Keep quorum during each phase and verify the new certificate names.

TLS encrypts transport only. It does not encrypt ordinary database records,
backups, application-returned secrets, or process memory. Managed-key envelopes
provide the separate protection described in [SECRETS.md](SECRETS.md).

Validation includes native h2/HTTP1 negotiation, untrusted roots, wrong DNS/IP
names, silent handshake deadlines, credential route separation, and
`node tests/e2e-tls.mjs`: three real TLS Raft members with follower forwarding,
SDK HTTPS pooling, SSE, fresh reads and durable retry receipts.
