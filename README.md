# c-holons

C SDK for Organic Programming.

The current C surface includes:

- `transport`: URI parsing plus low-level listeners and dial helpers for
  `tcp`, `unix`, `stdio`, and `mem`
- `serve`: standard `--listen` / `--port` handling and a connection loop
- `identity`: `holon.yaml` parsing
- `discover`: filesystem discovery for local, `$OPBIN`, and cache roots

## Build and test

```bash
make test
```

Equivalent manual command:

```bash
clang -std=c11 -Wall -Wextra -pedantic -I include src/holons.c test/holons_test.c -o test_runner
./test_runner
```

## API surface

| API | Purpose |
|-----|---------|
| `holons_parse_flags` | Parse `--listen` / `--port` with default `tcp://:9090` |
| `holons_parse_uri` | Parse transport URI into a normalized struct |
| `holons_listen` / `holons_accept` | Listener + connection lifecycle |
| `holons_mem_dial` | Client-side dial for `mem://` |
| `holons_dial_tcp` | Client-side dial for TCP listeners |
| `holons_dial_stdio` | Wrap current process stdin/stdout as a client connection |
| `holons_serve` | Standard connection loop for a holon `serve` command |
| `holons_parse_holon` | Parse identity fields from `holon.yaml` |
| `holons_discover*` | Discover holons by scanning manifests |
| `holons_find_by_slug` / `holons_find_by_uuid` | Resolve a discovered holon |

## Notes

- The C public header stays intentionally small and C-first.
- Higher-level gRPC runner utilities live in `bin/` wrapper scripts that
  are owned by the SDK.
- `bin/grpc-bridge` is the standard daemon-facing runner when a C holon
  implements unary RPC logic over simple HTTP/JSON and still needs to
  satisfy the ecosystem-wide gRPC + `Describe` contract.
