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

Upload a large payload through a resumable HTTP session:

```
size=$(wc -c < big.bin)
digest=$(sha256sum big.bin | cut -d' ' -f1)
session=$(curl -sS -X POST http://127.0.0.1:8080/v1/uploads \
  -d "{\"key\":\"docs/big.bin\",\"size\":$size,\"sha256\":\"$digest\"}" \
  | sed 's/.*"session":"\([0-9a-f]*\)".*/\1')
head -c 1048576 big.bin | curl -sS -X PUT \
  "http://127.0.0.1:8080/v1/uploads/$session?offset=0" --data-binary @-
tail -c +1048577 big.bin | curl -sS -X PUT \
  "http://127.0.0.1:8080/v1/uploads/$session?offset=1048576" --data-binary @-
curl -sS -X POST "http://127.0.0.1:8080/v1/uploads/$session/complete"
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

### Conditional writes

`put`, `delete` and `begin_upload` accept an optional `expected_sha256`
keyword that turns the call into a compare-and-swap on the key's current
digest. Omitting it keeps the unconditional behaviour; passing `None`
requires the key to be absent; passing the 64 lowercase hex digits of a
digest requires the key to exist with exactly that digest in its metadata.
Any other value raises `ObjectStoreError("invalid precondition")`. Only the
digest of that one key is compared — the content type and other keys play no
role. A failed condition raises `ObjectStoreError("precondition failed")`
and changes no object, upload session or blob; a `delete` whose "must be
absent" condition is satisfied still reports the missing key with the usual
not-found error. The check and the write or delete it guards are a single
locked operation, so of two concurrent writers relying on the same old
digest only one can succeed.

```python
store.put("docs/a.txt", b"v2", expected_sha256=sha256_of_v1)  # update if unchanged
store.put("docs/b.txt", b"new", expected_sha256=None)         # create only if absent
store.delete("docs/a.txt", expected_sha256=sha256_of_v2)      # delete if unchanged
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
An optional `expected_sha256` precondition (same meaning as in `put`) is
stored with the declaration and survives reopening the data directory; it is
never checked at creation or append time, only when the completed upload is
published. Creating a session changes no object, and sessions with their
confirmed progress survive reopening the data directory. `append_upload` accepts the
same byte types as `put`: a new chunk must start exactly at the current end
and may not exceed the declared size; re-sending identical bytes fully inside
the received range succeeds without growing, while different bytes, chunks
overlapping the received end, gaps and empty chunks anywhere but at the end
all raise `ObjectStoreError`. `complete_upload` publishes the object only when
the received length and the SHA-256 of the full content match the declaration
(zero-byte objects included); until then the key keeps its old value and no
new blob appears. A session declared with a precondition then evaluates it
against the object state at publish time: a failed condition raises
`ObjectStoreError("precondition failed")` and keeps the confirmed bytes and
the incomplete state, so the session can be completed again or aborted. Appending after completion fails, repeating a completed
`complete_upload` returns the first result without overwriting later writes
or deletes of the key, and `abort_upload` removes the session (returning
`None`) without deleting any published object. An interrupted append leaves
either the old progress or a whole new chunk, and an interrupted complete
leaves either the old object or the fully published new one; after recovery
the `completed` flag always agrees with what was published. Sessions on the
same key are independent and publish in completion order. Garbage collection
never touches data needed by incomplete sessions; once a session completes or
is aborted, its blob follows the normal object-reference rules and the
completion record itself keeps nothing alive. The command line interface is
unchanged by this feature; the HTTP session surface is documented below.

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
`404 {"error":"not found"}`. Conditional-write headers on object `PUT`,
object `DELETE` and `POST /v1/uploads` follow strict formats: `If-Match`
accepts exactly one double-quoted lowercase digest (`"<sha256>"`) and
requires the key to exist with that digest, `If-None-Match` accepts only `*`
and requires the key to be absent. Both headers together, a repeated header
or a malformed value returns `400 {"error":"invalid precondition"}`, and a
condition that does not hold returns `412 {"error":"precondition failed"}`
without changing any object, session or blob. Object `GET` and `HEAD`
responses carry an `ETag` header with the current digest in double quotes;
other successful responses are unchanged. Upload session conflicts (gaps, bytes past the
received end or the declared size, conflicting re-sends, empty chunks away
from the end, appending after completion, or completing with the wrong length
or hash) return `409` and never change progress or the target object; a
missing, non-integer or negative `Content-Length` on session creation or
chunk append returns `411 {"error":"length required"}`; a request carrying
`Transfer-Encoding` returns `400` and a body shorter than its declared
`Content-Length` also returns `400`. Every object GET and blob GET recomputes
the SHA-256 of the bytes actually on disk before serving them; object GET also
checks the byte count against the metadata `size`. A failed check returns
`500 {"error":"corrupted blob"}` with no object bytes, and an I/O error on an
existing blob returns `500 {"error":"blob io error"}`; upload session I/O
failures return `500 {"error":"upload io error"}`. Re-PUTting an object
whose digest file is corrupted restores that file atomically from the complete
request body, which also repairs every other key sharing that digest; HEAD,
listings and digest listings remain metadata-only and perform no content
checks.

| Method | Path | Success | Error codes |
| --- | --- | --- | --- |
| GET | `/healthz` | 200 `{"ok": true}` | 405 |
| PUT | `/v1/objects/{key}` | 201 `{"key","sha256","size"}` on first store; 200 with the same body when the same bytes are stored again | 400 invalid key or invalid precondition, 411 missing Content-Length, 412 precondition failed, 405, 500 blob io error |
| GET | `/v1/objects/{key}` | 200 raw bytes, headers `Content-Type`, `X-Content-Sha256` and `ETag` | 404 unknown key, 400 invalid key, 405, 500 corrupted blob / blob io error |
| HEAD | `/v1/objects/{key}` | 200 no body, headers `Content-Length`, `X-Content-Sha256` and `ETag` | 404 unknown key, 400 invalid key, 405 |
| DELETE | `/v1/objects/{key}` | 204 no body | 404 unknown key, 400 invalid key or invalid precondition, 412 precondition failed, 405 |
| GET | `/v1/objects?prefix=&after=&limit=` | 200 `{"items":[{"key","sha256","size","content_type"}],"next_after":<str or null>}` | 400 invalid limit, 400 invalid prefix, 400 invalid after, 405 |
| GET | `/v1/blobs/{sha256}` | 200 raw bytes of that content, header `X-Content-Sha256` | 400 invalid sha256, 404 unknown digest, 405, 500 corrupted blob / blob io error |
| POST | `/v1/uploads` | 201 `{"session":"<id>"}`; body is a UTF-8 JSON object with required `key`, `size`, `sha256` and optional `content_type` (default `null`); accepts the conditional-write headers | 400 invalid json / fields or invalid precondition, 411 missing/non-integer/negative Content-Length, 400 Transfer-Encoding / truncated body, 405, 500 upload io error |
| GET | `/v1/uploads/{session}` | 200 `{"key","size","sha256","content_type","offset","completed"}` | 404 unknown or aborted session, 405 |
| PUT | `/v1/uploads/{session}?offset=<n>` | 200 `{"offset":<confirmed length>}`; body holds the raw chunk bytes and `offset` must be a unique non-negative decimal integer | 400 invalid/truncated framing, 404 unknown or aborted session, 409 gap/overlap/overflow/conflict/empty-chunk/completed, 411 missing/non-integer/negative Content-Length, 405, 500 upload io error |
| POST | `/v1/uploads/{session}/complete` | 200 `{"sha256","size","content_type"}` (the first result on repeat completion); no body required | 404 unknown or aborted session, 409 incomplete or hash mismatch, 412 precondition failed, 400 bad framing, 405, 500 blob/upload io error |
| DELETE | `/v1/uploads/{session}` | 204 no body; aborts the session and keeps any published object | 404 unknown or aborted session, 405 |

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

Upload session notes:

* `POST /v1/uploads` validates exactly like `begin_upload`: `key` is a
  non-empty slash-separated name without `.`/`..` parts or NUL bytes, `size`
  a non-negative JSON integer (booleans rejected), `sha256` 64 lowercase hex
  digits, and `content_type` a string when present. Extra fields are rejected.
  Creating a session never creates or changes an object. An `If-Match` or
  `If-None-Match: *` header is stored with the session and evaluated only
  when the upload is completed, against the object state at publish time; a
  failed condition returns 412 and keeps the session's confirmed progress,
  so it can be completed again or aborted.
* `PUT` appends follow the same rules as `append_upload`: a chunk must start
  at the current received end and may not run past the declared size; a chunk
  fully inside the received range succeeds at 200 without growing progress
  when its bytes match and gets 409 otherwise; empty chunks are accepted only
  at the current end. The 200 body always reports the confirmed length.
* Completing publishes the object only when the received length and the
  SHA-256 of the full content match (zero-byte objects included); repeating
  completion of the same session returns the first result without
  overwriting later writes or deletes of the key. Sessions on the same key
  publish in completion order; aborting a session keeps the object it
  published.
* Session state lives in the same on-disk files as the Python API, so a
  session created through one entry can be appended to, completed, inspected
  or aborted through the other, and progress survives a server restart.
* Session requests are framed strictly by `Content-Length`; chunked request
  bodies are never accepted. A `GET`/`DELETE` status or abort call needs no
  body, but any `Content-Length` it sends must be a valid non-negative
  decimal integer and the declared bytes are consumed.

## Storage layout

```
<data-dir>/
  index.json            # {"version":1,"objects":{"<key>":{"sha256","size","content_type"}},
                        #  "uploads":{"<session>":{"key","entry"}}} (only while non-empty)
  blobs/<sha256>        # raw object bytes, one file per distinct digest
  uploads/<session>.json  # active upload declaration plus confirmed offset (lazy directory);
                          #  carries "expected_sha256" when the session was created with a precondition
  uploads/<session>.part  # received bytes of an active upload
```

## Limits of this seed

The following long-term goals are intentionally not implemented yet: object
versioning and retention policies, lifecycle policies on top of the existing
garbage collection of unreferenced blobs, quotas and rate limiting, consistent
hashing and rebalancing, erasure coding and repair, signed URLs and access
control, cross-region replication and end-to-end audit logging. Resumable
uploads are exposed through the Python API and the HTTP surface; the command
line interface does not expose them yet. Authentication is out of scope: the
server trusts every caller.
