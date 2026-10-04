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
python3 -m objstore --data-dir ./objstore_data gc
python3 -m objstore --data-dir ./objstore_data gc --execute
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
result = store.collect_garbage()              # preview only
store.collect_garbage(dry_run=False)          # actually delete
```

### Resumable uploads

Large or unreliable transfers can be uploaded in chunks through a persistent
session instead of a single `put`:

```python
session = store.begin_upload("docs/big.bin", size, sha256_of_full_content,
                             content_type="application/octet-stream")
store.upload_status(session)          # {"key","size","sha256","content_type","offset","completed"}
store.append_upload(session, 0, first_chunk)      # returns the received length
store.append_upload(session, len(first_chunk), second_chunk)
entry = store.complete_upload(session)            # publishes, returns put metadata
store.abort_upload(session)                       # or drop the session entirely
```

`begin_upload` validates the key and content type exactly like `put`, requires
a non-negative integer `size` (booleans excluded) and the 64 lowercase hex
digits of the full content's SHA-256, and returns a unique string session id.
Creating a session changes no object, and sessions with their confirmed
progress survive reopening the data directory. `append_upload` accepts the
same byte types as `put`: a new chunk must start exactly at the current end
and may not exceed the declared size; re-sending identical bytes fully inside
the received range succeeds without growing, while different bytes, chunks
overlapping the received end, gaps and empty chunks anywhere but at the end
all raise `ObjectStoreError`. `complete_upload` publishes the object only when
the received length and the SHA-256 of the full content match the declaration
(zero-byte objects included); until then the key keeps its old value and no
new blob appears. Appending after completion fails, repeating a completed
`complete_upload` returns the first result without overwriting later writes
or deletes of the key, and `abort_upload` removes the session (returning
`None`) without deleting any published object. An interrupted append leaves
either the old progress or a whole new chunk, and an interrupted complete
leaves either the old object or the fully published new one; after recovery
the `completed` flag always agrees with what was published. Sessions on the
same key are independent and publish in completion order. Garbage collection
never touches data needed by incomplete sessions; once a session completes or
is aborted, its blob follows the normal object-reference rules and the
completion record itself keeps nothing alive. The HTTP and command line
interfaces are unchanged by this feature.

`collect_garbage(dry_run=True)` returns
`{"digests": [...], "bytes": <int>, "dry_run": <bool>}`: the digests sorted
lexicographically and the sum of the files' actual byte sizes (zero-byte
blobs included). A dry run reports the candidates and changes nothing;
`dry_run=False` deletes the candidates and reports what that call removed.
References are recomputed from the current index on every call, so content
still shared by any key is kept, while content orphaned by an overwrite, by
deleting the last reference, or never indexed at all is collectable. Only
regular files directly in `blobs/` whose names are 64 lowercase hex digits
are eligible; other names, temporary files, subdirectories and symlinks are
preserved and symlink targets are never followed. Unreferenced files are
deleted without validating their content, and missing or corrupted blobs
that are still referenced do not block other candidates. A non-boolean
`dry_run` raises `ObjectStoreError("invalid dry_run")`, and any scanning,
stating or deletion I/O failure raises `ObjectStoreError("gc io error")`; a
failed scan deletes nothing, and a failure mid-deletion leaves the completed
deletions in place so a retry collects the rest. Garbage collection runs
under the same lock as object reads and writes within one store instance.


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
  because other keys may still reference the same digest. Run `gc` (preview by
  default, `gc --execute` to delete) to remove the blobs no object references.
  The success line is
  `{"ok":true,"digests":[...],"bytes":<int>,"dry_run":<bool>}`.

## Storage layout

```
<data-dir>/
  index.json            # {"version":1,"objects":{"<key>":{"sha256","size","content_type"}},
                        #  "uploads":{"<session>":{"key","entry"}}} (only while non-empty)
  blobs/<sha256>        # raw object bytes, one file per distinct digest
  uploads/<session>.json  # active upload declaration plus confirmed offset (lazy directory)
  uploads/<session>.part  # received bytes of an active upload
```

## Limits of this seed

The following long-term goals are intentionally not implemented yet: object
versioning and retention policies, lifecycle policies on top of the existing
garbage collection of unreferenced blobs, quotas and rate limiting, consistent
hashing and rebalancing, erasure coding and repair, signed URLs and access
control, cross-region replication and end-to-end audit logging. Resumable
uploads exist only in the Python API; the HTTP and command line surfaces do
not expose them yet. Authentication is out of scope: the server trusts every
caller.
