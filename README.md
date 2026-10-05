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
which makes it a suitable frozen baseline for later work on lifecycle and
garbage collection, metadata indexing and listing, quotas and rate limiting,
consistent hashing and rebalancing, erasure coding and repair, signed URLs
and access control, cross-region replication and end-to-end audit.

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
`./objstore_data`. `put`, `get`, `list` and `delete` also accept `--tenant`
(default `default`) to pick the tenant namespace. Every command prints exactly
one line of JSON on stdout and exits 0 on success, or prints one line of JSON
on stderr and exits non-zero on failure.

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

### Tenants

Every object key lives in a tenant namespace. Two tenants may use the same
key for independent objects: reads, writes, deletes, conditional writes and
listings of one tenant never see or affect another tenant's objects. Tenants
need no provisioning — a tenant exists as soon as it has objects or sessions,
and listing a tenant without objects returns an empty page. A tenant name is
1–64 characters of lowercase letters, digits, `_` or `-`, starting with a
letter or digit; anything else raises `ObjectStoreError("invalid tenant: ...")`
(Python), returns `400 {"error":"invalid tenant ..."}` (HTTP) or the usual
CLI failure line with a non-zero exit, and changes no data.

The Python API takes an optional `tenant` keyword (default `"default"`) on
`put`, `get`, `head`, `delete`, `list_objects` and every upload-session
operation:

```python
store.put("docs/a.txt", b"hello", tenant="acme")
payload, head = store.get("docs/a.txt", tenant="acme")
page = store.list_objects(prefix="docs/", tenant="acme")
session = store.begin_upload("docs/big.bin", size, digest, tenant="acme")
```

The HTTP object and upload-session endpoints select the tenant through the
optional `X-Objstore-Tenant` request header (default `default`); a repeated
header or an invalid name returns `400 {"error":"invalid tenant ..."}` and
takes precedence over every other request error. The CLI commands `put`,
`get`, `list` and `delete` accept an optional `--tenant` flag.

A conditional write compares only the digest of the key in the same tenant,
and an upload session belongs to the tenant fixed at creation: its status,
append, complete and abort operations must name that tenant, and a valid
session id of another tenant behaves exactly like an unknown session id
(`ObjectStoreError` / `404`) without revealing any state. Blobs stay global —
identical content is stored once across all tenants, `blob`/`blob_digests`
and `GET /v1/blobs/{sha256}` keep their global meaning, and garbage
collection only reclaims content no tenant references anymore. Directories
written by older versions (objects, sessions and completion records without
tenant information) belong to the `default` tenant after the upgrade.

### Object versioning and retention

Versioning is a per-tenant toggle, off by default. While it is off, the
Python API, the HTTP surface, the CLI and every response are exactly as
before; the toggle only affects new writes and never deletes recorded
history implicitly, so indexes written by older versions need no migration
and gain no fabricated history.

```python
store.set_versioning(True, tenant="acme")        # enable for one tenant
store.get_versioning(tenant="acme")              # -> True
entry = store.put("docs/a.txt", b"v1", tenant="acme")
entry["version_id"]                              # immutable id of the new version
page = store.list_versions("docs/a.txt", tenant="acme")
data, meta = store.get_version("docs/a.txt", entry["version_id"], tenant="acme")
store.delete_version("docs/a.txt", entry["version_id"], tenant="acme")
```

While versioning is enabled for a tenant, every `put` and every completed
resumable upload appends an immutable version record (`version_id`, digest,
size, content type and UTC `created_at`) and makes it the current version;
`put`/`complete_upload` then also return the `version_id`. `get`, `head`
and `list_objects` still return only the current version, and `delete` only
removes the current pointer — the key disappears from listings while its
whole recorded history stays readable through the version operations.

`list_versions(key, after=None, limit=100, tenant=...)` returns the newest
version first, each item carrying `version_id`, `sha256`, `size`,
`created_at` and `current`, and paginates with the same `after`/`limit`
convention as `list_objects` (the cursor is a `version_id`).
`get_version(key, version_id, tenant=...)` verifies the blob's digest and
size exactly like `get`. `delete_version` removes one recorded version:
deleting a historical version leaves the current object untouched, deleting
the current version promotes the newest remaining version, and deleting the
last version makes the key disappear entirely. Calling any of these while
versioning is disabled for the tenant raises
`ObjectStoreError("versioning disabled")`; an unknown key or version raises
`not found`, and a malformed `version_id` raises `invalid version`.

A retention policy is saved per tenant with
`store.set_retention(max_versions=..., max_age_seconds=..., tenant=...)`:
both limits must be non-negative integers and at least one is required
(`invalid retention` otherwise); `get_retention` returns the saved policy
(`None` for unset limits). `store.purge_retention(dry_run=True, tenant=...)`
applies the policy to the tenant's recorded history: the current version of
every key is always kept, and versions beyond the newest `max_versions` per
key or older than `max_age_seconds` (compared against the current UTC time)
are deleted. It returns `{"versions": [{"key", "version_id"}...], "bytes":
<int>, "dry_run": <bool>}` with the removed pairs sorted by `version_id`; a
dry run (the default) changes nothing, a non-boolean `dry_run` raises
`invalid dry_run`, a missing policy raises `retention not configured`, and
file I/O failures raise `version io error`. Garbage collection treats blobs
referenced by any version record, any current object and any incomplete
upload session as live, so a version's blob becomes collectable only after
its record is removed by `delete_version` or `purge_retention`.

The HTTP surface mirrors the Python API; the tenant is selected with the
usual `X-Objstore-Tenant` header:

| Method | Path | Success | Error codes |
| --- | --- | --- | --- |
| GET | `/v1/versioning` | 200 `{"enabled": <bool>}` | 405 |
| PUT | `/v1/versioning` | 200 `{"enabled": <bool>}`; body `{"enabled": <bool>}` | 400 invalid json / invalid versioning, 405, 411 |
| GET | `/v1/objects/{key}/versions?after=&limit=` | 200 `{"items":[{"version_id","sha256","size","created_at","current"}],"next_after":<str or null>}` | 400 invalid key / invalid limit / invalid after / versioning disabled, 405 |
| GET | `/v1/objects/{key}/versions/{version_id}` | 200 raw bytes, headers `Content-Type`, `X-Content-Sha256` and `ETag` | 400 invalid key / invalid version / versioning disabled, 404 unknown key or version, 405, 500 corrupted blob / blob io error |
| DELETE | `/v1/objects/{key}/versions/{version_id}` | 204 no body | 400 invalid key / invalid version / versioning disabled, 404 unknown key or version, 405, 500 version io error |
| GET | `/v1/retention` | 200 `{"max_versions": <int or null>, "max_age_seconds": <int or null>}` | 405 |
| PUT | `/v1/retention` | 200 saved policy; body holds `max_versions` and/or `max_age_seconds` | 400 invalid json / invalid retention, 405, 411 |
| POST | `/v1/retention/purge` | 200 `{"versions":[{"key","version_id"}...],"bytes":<int>,"dry_run":<bool>}`; optional body `{"dry_run": <bool>}` (default true) | 400 invalid json / invalid dry_run, 404 retention not configured, 405, 500 version io error |

When versioning is enabled for the tenant, `PUT /v1/objects/{key}` and
`POST /v1/uploads/{session}/complete` also return the new `version_id` in
their JSON response; when it is disabled their responses are unchanged.

### Conditional writes

`put`, `delete` and `begin_upload` take an optional keyword
`expected_sha256` that makes the write or delete an atomic compare-and-swap on
the target key's current digest:

* omitted entirely keeps the original unconditional behaviour;
* an explicit `None` requires the key not to exist;
* a string of 64 lowercase hex digits requires the key to exist with exactly
  that metadata digest.

Only that key's digest is compared; the content type and every other key are
irrelevant. Any other value raises
`ObjectStoreError("invalid precondition")`. When the condition does not hold,
the operation raises `ObjectStoreError("precondition failed")` and changes
neither the object nor (for uploads) the session progress or any blob; a
satisfied absent-condition `delete` still raises the usual
`ObjectStoreError("not found: ...")`. The check and the mutation are one
indivisible operation under the store lock, so two concurrent writes that
both expect the same old digest cannot both succeed.

```python
store.put("docs/a.txt", b"hello", expected_sha256=None)          # create only
store.put("docs/a.txt", b"hello!", expected_sha256=head["sha256"])  # CAS update
store.delete("docs/a.txt", expected_sha256=head["sha256"])      # delete if unchanged
```

For a resumable session the condition passed to `begin_upload` is saved with
the session and is not checked while the session is created or appended to;
it is evaluated once, atomically with the publish, against the object's state
at completion time (after the length and hash checks pass). A failed
precondition leaves the confirmed bytes and the incomplete session in place,
so it can be completed again or aborted; the condition still applies after
reopening the data directory, while session files written by older versions
stay unconditional.

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
`404 {"error":"not found"}`. Object PUT/DELETE and `POST /v1/uploads` accept
conditional headers: `If-Match` must be a single double-quoted lowercase
digest (`If-Match: "<sha256>"`) requiring the key to hold that digest, and
`If-None-Match: *` requires the key to be absent. Both headers together, a
repeated header, or any other value returns
`400 {"error":"invalid precondition"}`; a condition that does not hold
returns `412 {"error":"precondition failed"}` without changing anything (an
absent-key `DELETE` still returns its `404 not found`). Request/body errors,
unknown sessions, and length/hash conflicts keep their usual status codes and
take precedence over the condition. Successful object GET and HEAD return an
`ETag` header holding the current digest in double quotes; no other success
response changes. Upload session conflicts (gaps, bytes past the
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
| PUT | `/v1/objects/{key}` | 201 `{"key","sha256","size"}` on first store; 200 with the same body when the same bytes are stored again | 400 invalid key / invalid precondition, 411 missing Content-Length, 405, 412 precondition failed, 500 blob io error |
| GET | `/v1/objects/{key}` | 200 raw bytes, headers `Content-Type`, `X-Content-Sha256` and `ETag` (`"<sha256>"`) | 404 unknown key, 400 invalid key, 405, 500 corrupted blob / blob io error |
| HEAD | `/v1/objects/{key}` | 200 no body, headers `Content-Length`, `X-Content-Sha256` and `ETag` | 404 unknown key, 400 invalid key, 405 |
| DELETE | `/v1/objects/{key}` | 204 no body | 404 unknown key, 400 invalid key / invalid precondition, 405, 412 precondition failed |
| GET | `/v1/objects?prefix=&after=&limit=` | 200 `{"items":[{"key","sha256","size","content_type"}],"next_after":<str or null>}` | 400 invalid limit, 400 invalid prefix, 400 invalid after, 405 |
| GET | `/v1/blobs/{sha256}` | 200 raw bytes of that content, header `X-Content-Sha256` | 400 invalid sha256, 404 unknown digest, 405, 500 corrupted blob / blob io error |
| POST | `/v1/uploads` | 201 `{"session":"<id>"}`; body is a UTF-8 JSON object with required `key`, `size`, `sha256` and optional `content_type` (default `null`); optional `If-Match`/`If-None-Match` save a publish-time condition | 400 invalid json / fields / invalid precondition, 411 missing/non-integer/negative Content-Length, 400 Transfer-Encoding / truncated body, 405, 500 upload io error |
| GET | `/v1/uploads/{session}` | 200 `{"key","size","sha256","content_type","offset","completed"}` | 404 unknown or aborted session, 405 |
| PUT | `/v1/uploads/{session}?offset=<n>` | 200 `{"offset":<confirmed length>}`; body holds the raw chunk bytes and `offset` must be a unique non-negative decimal integer | 400 invalid/truncated framing, 404 unknown or aborted session, 409 gap/overlap/overflow/conflict/empty-chunk/completed, 411 missing/non-integer/negative Content-Length, 405, 500 upload io error |
| POST | `/v1/uploads/{session}/complete` | 200 `{"sha256","size","content_type"}` (the first result on repeat completion); no body required | 404 unknown or aborted session, 409 incomplete or hash mismatch, 412 saved precondition failed, 400 bad framing, 405, 500 blob/upload io error |
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
  Creating a session never creates or changes an object.
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
* `If-Match: "<sha256>"` or `If-None-Match: *` on `POST /v1/uploads` saves a
  condition exactly like `begin_upload(expected_sha256=...)`: it is not
  evaluated while the session is created or appended to, but once at
  completion, atomically with the publish and after the length/hash checks.
  Failure is `412 {"error":"precondition failed"}`, leaves the confirmed
  bytes and incomplete session usable, and remains in force after a restart.
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
                        #  "tenants":{"<tenant>":{"<key>":{...}}} (non-default tenants,
                        #  only while non-empty),
                        #  "uploads":{"<session>":{"key","entry","tenant"?,"version_id"?}}
                        #  (only while non-empty),
                        #  "versioning":{"<tenant>":true} (only enabled tenants),
                        #  "retention":{"<tenant>":{"max_versions"?,"max_age_seconds"?}},
                        #  "versions":{"<tenant>":{"<key>":{"current":<id or null>,
                        #  "versions":[{"version_id","sha256","size","content_type",
                        #  "created_at"}...]}}} (only while non-empty)}
  blobs/<sha256>        # raw object bytes, one file per distinct digest
  uploads/<session>.json  # active upload declaration plus confirmed offset, tenant
                          # (non-default only) and optional expected_sha256 publish
                          # condition (lazy directory)
  uploads/<session>.part  # received bytes of an active upload
```

The flat `objects` map holds the `default` tenant, so indexes written before
tenants existed need no migration; the same applies to session files and
completion records without a `tenant` field. Indexes written before
versioning existed have no `versioning`, `retention` or `versions` maps and
simply load with versioning disabled and no recorded history.

## Limits of this seed

The following long-term goals are intentionally not implemented yet:
lifecycle policies on top of the existing
garbage collection of unreferenced blobs, quotas and rate limiting, consistent
hashing and rebalancing, erasure coding and repair, signed URLs and access
control, cross-region replication and end-to-end audit logging. Resumable
uploads are exposed through the Python API and the HTTP surface; the command
line interface does not expose them yet. Authentication is out of scope: the
server trusts every caller.
