# solo06 - Multi-Tenant Content-Addressed Object Store

This repository is a small but real backend skeleton for multi-tenant object
storage with content addressing. Every object key maps to the SHA-256 digest of
its bytes, blobs are written once under `<data-dir>/blobs/<sha256>` so identical
payloads are deduplicated no matter how many keys reference them, and object
metadata is kept in `<data-dir>/index.json` and rewritten atomically (temporary
file plus `os.replace`) so a crash never leaves a half-written index behind. The
same core is exposed three ways: as a Python API (`ContentAddressedStore`), as a
standard-library HTTP service, and as a command line tool. It uses only the
Python standard library, needs no network access and behaves deterministically,
which makes it a suitable frozen baseline for later work on chunked and
resumable uploads, object versioning and retention, lifecycle and garbage
collection, metadata indexing and listing, quotas and rate limiting, consistent
hashing and rebalancing, erasure coding and repair, signed URLs and access
control, cross-region replication and end-to-end audit.

## Requirements

* Python 3.10 or newer (developed and verified on Python 3.12).
* No third-party packages, no network access required.

## Usage

Start the HTTP server (run from the repository root):

```
python3 -m objstore --data-dir ./objstore_data serve --host 127.0.0.1 --port 8080
```

The server prints one line of JSON before it starts serving, for example
`{"addr":"127.0.0.1:8080","data_dir":"/abs/path/objstore_data","ok":true}`, and
then runs until it receives SIGINT or SIGTERM.

Store and fetch data over HTTP:

```
printf 'hello' | curl -sS -X PUT --data-binary @- -H 'Content-Type: text/plain' \
  http://127.0.0.1:8080/v1/objects/a.txt
curl -sS http://127.0.0.1:8080/v1/objects/a.txt
curl -sS 'http://127.0.0.1:8080/v1/objects?prefix=&limit=100'
```

Use the command line interface instead of the server:

```
python3 -m objstore --data-dir ./objstore_data put --key docs/a.txt --file ./a.txt
python3 -m objstore --data-dir ./objstore_data get --key docs/a.txt --out ./copy.txt
python3 -m objstore --data-dir ./objstore_data list --prefix docs/ --limit 100
python3 -m objstore --data-dir ./objstore_data delete --key docs/a.txt
python3 -m objstore --data-dir ./objstore_data gc             # preview only
python3 -m objstore --data-dir ./objstore_data gc --execute   # actually delete
```

The global `--data-dir` option must appear before the subcommand; it defaults to
`./objstore_data`. Every command prints exactly one line of JSON on stdout and
exits 0 on success, or prints one line of JSON on stderr and exits non-zero on
failure.

Use the Python API directly:

```python
from objstore import ContentAddressedStore

store = ContentAddressedStore("./objstore_data")
entry = store.put("docs/a.txt", b"hello", content_type="text/plain")
payload, head = store.get("docs/a.txt")
page = store.list_objects(prefix="docs/", after=None, limit=100)
store.collect_garbage()              # preview: {"digests": [...], "bytes": n, "dry_run": True}
store.collect_garbage(dry_run=False) # delete unreferenced blobs for real
```

## Garbage collection

Deleting a key removes only its metadata entry; the blob bytes stay on disk.
`ContentAddressedStore.collect_garbage(dry_run=True)` reclaims that space. It
recomputes the live set from the current object index on every call, so:

* blobs referenced by any object are always kept, including blobs shared by
  several keys;
* the old bytes of an overwritten object, the bytes left after the last
  reference is deleted, and complete blobs that were never indexed are all
  collectable;
* a preview (`dry_run=True`, the default, or `gc` with no flags) reports the
  candidates without touching any stored file; an execution
  (`dry_run=False`, or `gc --execute`) returns the digests that were actually
  removed.

Both forms return `{"digests": [...], "bytes": n, "dry_run": bool}` with
digests in lexicographic order and `bytes` equal to the sum of the actual
sizes of the reported files (zero-byte candidates count). An empty result is
`[]` / `0`.

Only regular files directly under `blobs/` whose names are exactly 64
lowercase hex digits are eligible. Other names, leftover temporary files,
subdirectories and symbolic links are preserved, and symlink targets are never
followed. Content is not verified before removing an unreferenced file, and a
referenced blob that is missing or corrupted neither blocks the other
candidates nor changes its metadata; ordinary read errors are reported as
before. A candidate that disappears while the scan or the deletes are running
is simply skipped and excluded from the result. Any other directory scan,
file-stat or delete I/O failure raises `ObjectStoreError("gc io error")`; a
failed scan removes nothing, while a failure midway through a delete leaves
the completed deletions in place and a later retry safely reaps the rest.
`dry_run` must be a boolean or `ObjectStoreError("invalid dry_run")` is raised
before anything is touched. Garbage collection and object reads and writes on
the same store instance are serialized by one lock, so content that is being
published or is still referenced is never deleted; content referenced after a
preview is retained by a later execution. Cross-process locking is out of
scope.

## Tests

```
python3 -m unittest discover -s tests -v
```

Run the command from the repository root so that `import objstore` resolves.

## HTTP API

Errors are always JSON: `{"error": "..."}`. Unknown keys return
`404 {"error":"not found"}`; a malformed digest returns
`400 {"error":"invalid sha256"}`; an out-of-range or non-numeric `limit`
returns `400 {"error":"invalid limit"}`; an unknown path returns
`404 {"error":"not found"}`. Every object GET and blob GET recomputes the
SHA-256 of the bytes actually on disk before serving them; object GET also
checks the byte count against the metadata `size`. A failed check returns
`500 {"error":"corrupted blob"}` with no object bytes, and an I/O error on an
existing blob returns `500 {"error":"blob io error"}`. Re-PUTting an object
whose digest file is corrupted restores that file atomically from the complete
request body, which also repairs every other key sharing that digest; HEAD,
listings and digest listings remain metadata-only and perform no content
checks.

| Method | Path | Success | Error codes |
| --- | --- | --- | --- |
| GET | `/healthz` | 200 `{"ok": true}` | 405 |
| PUT | `/v1/objects/{key}` | 201 `{"key","sha256","size"}` on first store; 200 with the same body when the same bytes are stored again | 400 invalid key, 411 missing Content-Length, 405, 500 blob io error |
| GET | `/v1/objects/{key}` | 200 raw bytes, headers `Content-Type` and `X-Content-Sha256` | 404 unknown key, 400 invalid key, 405, 500 corrupted blob / blob io error |
| HEAD | `/v1/objects/{key}` | 200 no body, headers `Content-Length` and `X-Content-Sha256` | 404 unknown key, 400 invalid key, 405 |
| DELETE | `/v1/objects/{key}` | 204 no body | 404 unknown key, 400 invalid key, 405 |
| GET | `/v1/objects?prefix=&after=&limit=` | 200 `{"items":[{"key","sha256","size","content_type"}],"next_after":<str or null>}` | 400 invalid limit, 400 invalid prefix, 400 invalid after, 405 |
| GET | `/v1/blobs/{sha256}` | 200 raw bytes of that content, header `X-Content-Sha256` | 400 invalid sha256, 404 unknown digest, 405, 500 corrupted blob / blob io error |

Notes:

* `PUT` reads exactly `Content-Length` bytes from the request body; chunked
  request bodies are not supported.
* `after` is an exclusive lower bound on the key, so the `next_after` value of a
  page can be passed straight back as `after` to fetch the next page.
* `limit` must be an integer between 1 and 1000.
* Deleting a key removes only the metadata entry; blob bytes remain on disk
  because other keys may still reference the same digest. Run
  `collect_garbage` (or the `gc` command) to reclaim unreferenced blobs.

## Storage layout

```
<data-dir>/
  index.json          # {"version":1,"objects":{"<key>":{"sha256","size","content_type"}}}
  blobs/<sha256>      # raw object bytes, one file per distinct digest
```

## Limits of this seed

The following long-term goals are intentionally not implemented yet: chunked and
resumable uploads, object versioning and retention policies, automatic lifecycle
expiry (manual garbage collection of unreferenced blobs is available via
`collect_garbage` / the `gc` command), quotas and rate limiting, consistent
hashing and rebalancing, erasure coding and repair, signed URLs and access
control, cross-region replication and end-to-end audit logging. Authentication
is out of scope: the server trusts every caller.
