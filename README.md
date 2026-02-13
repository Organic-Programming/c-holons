---
# Cartouche v1
title: "c-holons - C SDK for Organic Programming"
author:
  name: "B. ALTER"
created: 2026-02-12
access:
  humans: true
  agents: false
status: draft
---
# c-holons

C SDK for Organic Programming.

The package mirrors the Go reference surface:
- `transport`: URI parsing + listeners for `tcp`, `unix`, `stdio`, `mem`, `ws`, `wss`
- `serve`: standard `--listen` / `--port` handling with signal-aware accept loop
- `identity`: `HOLON.md` frontmatter parsing

## Build and test

```bash
clang -std=c11 -Wall -Wextra -pedantic -I include src/holons.c test/holons_test.c -o test_runner
./test_runner
```

## API surface

| API | Purpose |
|-----|---------|
| `holons_parse_flags` | Parse `--listen` / `--port` with default `tcp://:9090` |
| `holons_parse_uri` | Parse transport URI into normalized struct |
| `holons_listen` / `holons_accept` | Listener + connection lifecycle |
| `holons_mem_dial` | Client-side dial for `mem://` |
| `holons_dial_tcp` | Client-side dial for TCP listeners |
| `holons_dial_stdio` | Wrap current process stdin/stdout as a client connection |
| `holons_serve` | Standard connection loop for a holon `serve` command |
| `holons_parse_holon` | Parse identity fields from `HOLON.md` frontmatter |

## Transport notes

| Scheme | Status |
|--------|--------|
| `tcp://` | Full listener implementation |
| `unix://` | Full listener implementation |
| `stdio://` | Single-connection listener using stdin/stdout |
| `mem://` | In-process socketpair listener + dial |
| `ws://` | URI-compatible listener (socket layer) |
| `wss://` | URI-compatible listener (socket layer) |

Cross-language smoke test:

- The C test runner starts a Go echo server from `sdk/go-holons` and verifies
  TCP round-trip via `holons_dial_tcp`.
