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
```

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
`404 {"error":"not found"}`. A blob whose on-disk bytes no longer match its
digest, or an object whose byte count disagrees with its metadata `size`,
returns `500 {"error":"corrupted blob"}`; an unreadable blob file or a failed
repair write returns `500 {"error":"blob io error"}`. Neither body ever
contains object bytes.

| Method | Path | Success | Error codes |
| --- | --- | --- | --- |
| GET | `/healthz` | 200 `{"ok": True}` | 405 |
| PUT | `/v1/objects/{key}` | 201 `{"key","sha256","size"}` on first store; 200 with the same body when the same bytes are stored again. If the shared blob file is present but corrupt, the incoming bytes repair it atomically before the key is updated | 400 invalid key, 411 missing Content-Length, 500 blob io error, 405 |
| GET | `/v1/objects/{key}` | 200 raw bytes, headers `Content-Type` and `X-Content-Sha256`; bytes are re-hashed on every read and must match the metadata `size` | 404 unknown key, 400 invalid key, 500 corrupted/blob io error, 405 |
| HEAD | `/v1/objects/{key}` | 200 no body, headers `Content-Length` and `X-Content-Sha256` (metadata only; content is not read or verified) | 404 unknown key, 400 invalid key, 405 |
| DELETE | `/v1/objects/{key}` | 204 no body | 404 unknown key, 400 invalid key, 405 |
| GET | `/v1/objects?prefix=&after=&limit=` | 200 `{"items":[{"key","sha256","size","content_type"}],"next_after":<str or null>}` (metadata only) | 400 invalid limit, 400 invalid prefix, 400 invalid after, 405 |
| GET | `/v1/blobs/{sha256}` | 200 raw bytes of that content, header `X-Content-Sha256`; the bytes are re-hashed and must equal the requested digest | 400 invalid sha256, 404 unknown/missing digest, 500 corrupted/blob io error, 405 |

Notes:

* `PUT` reads exactly `Content-Length` bytes from the request body; chunked
  request bodies are not supported.
* `after` is an exclusive lower bound on the key, so the `next_after` value of a
  page can be passed straight back as `after` to fetch the next page.
* `limit` must be an integer between 1 and 1000.
* Deleting a key removes only the metadata entry; blob bytes remain on disk
  because other keys may still reference the same digest.
* Read failures never delete a blob or rewrite metadata. Re-putting the
  original content of a corrupt blob restores every key that shares the digest
  (keys whose own metadata `size` is wrong keep that metadata and still fail
  `GET` until their metadata is corrected). Repair uses the same atomic
  temp-file-plus-`os.replace` write as the index, so an interrupted repair
  leaves either the old bytes or the complete new bytes, never a mix.

## Storage layout

```
<data-dir>/
  index.json          # {"version":1,"objects":{"<key>":{"sha256","size","content_type"}}}
  blobs/<sha256>      # raw object bytes, one file per distinct digest
```

## Limits of this seed

The following long-term goals are intentionally not implemented yet: chunked and
resumable uploads, object versioning and retention policies, lifecycle and
garbage collection of unreferenced blobs, quotas and rate limiting, consistent
hashing and rebalancing, erasure coding and repair, signed URLs and access
control, cross-region replication and end-to-end audit logging. Authentication
is out of scope: the server trusts every caller.
