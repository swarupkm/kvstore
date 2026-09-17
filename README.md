# kvstore

A key-value store built from scratch in Go as a learning project — a TCP server with a
write-ahead log, a B-tree key index, leader/replica replication, and an MVCC transaction
layer, plus the beginnings of a small query language.

## Features

- **TCP text protocol** — line-based commands, one connection per client (`internal/server`)
- **Write-ahead log (WAL)** — every mutation is appended before it's applied, with
  group-commit fsync (every 1ms) and replay-on-startup for crash recovery (`internal/store/wal.go`)
- **B-tree key index** — backs `KEYS`, `RANGE`, and `PREFIX` lookups (`internal/store/index.go`,
  using [google/btree](https://github.com/google/btree))
- **TTL / expiry** — `SETEX` sets a key with a time-to-live; a background sweeper reaps
  expired entries, and TTLs survive WAL replay and compaction
- **WAL compaction** — collapses the log to the current live key set, both on-demand
  (`COMPACT`) and on a timer, plus on graceful shutdown
- **Replication** — a primary streams WAL entries to connected replicas over TCP; replicas
  reconnect and resume from their last applied offset, with a ring buffer on the primary
  for catch-up after brief disconnects (`internal/replication`)
- **MVCC transactions** — `BEGIN`/`COMMIT`/`ROLLBACK` give snapshot-isolated read-write
  transactions with conflict detection, backed by per-key version chains and background
  garbage collection of unreachable versions (`internal/mvcc`)
- **Query language (in progress)** — a lexer/parser for a small SQL-like syntax
  (`SET`, `GET`, `DELETE`, `SELECT VALUE WHERE ...`) that produces an AST
  (`internal/query`); not yet wired into the server
- **pprof profiling** — the server exposes `net/http/pprof` on `:6060`

## Project layout

```
cmd/
  server/   TCP server entrypoint
  client/   interactive REPL client
  bench/    simple concurrent load generator
internal/
  store/         core map store, WAL, B-tree index, metrics
  mvcc/          multi-version store and transaction handles
  replication/   primary/replica streaming and ring buffer
  query/         lexer, parser, and AST for the query language
  server/        TCP command handler
  config/        JSON config loading
```

## Getting started

Requires Go 1.23+.

```sh
go build ./...
go test ./...
```

Run a standalone server with the default config (`:5379`, WAL at `kvstore.wal`):

```sh
go run ./cmd/server
```

Or point it at a config file:

```sh
go run ./cmd/server config.json
```

Connect with the bundled client:

```sh
go run ./cmd/client
```

### Running primary + replica

```sh
go run ./cmd/server config-primary.json   # :5379, replication on :5380
go run ./cmd/server config-replica.json   # :5381, replicates from :5380
```

### Config fields

| Field                          | Description                                      |
|--------------------------------|---------------------------------------------------|
| `address`                       | TCP address the server listens on                 |
| `wal_path`                      | path to the write-ahead log file                  |
| `compaction_interval_seconds`   | how often auto-compaction runs                    |
| `replication_addr`              | if set, run as a primary and stream WAL here       |
| `replica_of`                    | if set, run as a replica of this primary address   |

## Protocol / commands

Connect over TCP and send one command per line; the server replies with one or more
lines depending on the command.

| Command                          | Description                                      |
|-----------------------------------|---------------------------------------------------|
| `SET <key> <value>`                | set a key                                          |
| `GET <key>`                        | get a key, or `NULL` if absent/expired             |
| `DEL <key>`                        | delete a key                                       |
| `SETEX <key> <seconds> <value>`    | set a key with a TTL                               |
| `INCR <key>`                       | atomically increment an integer key                |
| `KEYS`                             | list all live keys                                 |
| `RANGE <start> <end>`              | list keys in `[start, end]`                        |
| `PREFIX <prefix>`                  | list keys starting with `prefix`                   |
| `COMPACT`                          | compact the WAL now                                |
| `INFO` / `STATS`                   | key count and op counters (`INFO` also shows replica offset) |
| `BEGIN`                            | start a read-write MVCC transaction                |
| `COMMIT`                           | commit the open transaction                        |
| `ROLLBACK`                         | discard the open transaction                        |

`SET`/`GET`/`DEL`/`SETEX` behave transactionally when issued between `BEGIN` and
`COMMIT`/`ROLLBACK`, buffering writes until commit.

## Benchmarking

`cmd/bench` spins up 20 workers doing 500 SET+GET pairs each against a running server:

```sh
go run ./cmd/server &
go run ./cmd/bench
```
