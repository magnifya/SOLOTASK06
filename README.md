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
control, cross-region replication and end-to-end audit. Multi-tenant namespaces,
conditional writes, resumable uploads, object versioning with retention,
per-tenant quotas, persistent object lifecycle policies, signed read
access policies and per-tenant fixed-window rate limiting are already
implemented. Per-tenant persistent audit logging with hash-chained
verification is implemented as well.

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
still shared by any key, any retained object version or any incomplete
upload session is kept, while content orphaned by an overwrite, by
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

### Object versioning and retention

Versioning is opt-in per tenant and defaults to off; while off, every API,
HTTP path and CLI command behaves exactly as before. The toggle only affects
new writes — enabling or disabling it never deletes or rewrites existing
objects or versions:

```python
store.set_versioning(True, tenant="acme")        # enable for one tenant
store.get_versioning(tenant="acme")              # -> True
```

While a tenant has versioning enabled, every `put` and every completed
resumable upload appends an immutable version (a random 32-hex-digit
`version_id`, the content metadata and a UTC `created_at`) to the key's
version list and makes it the current version; the returned metadata carries
the new `version_id`. `get`, `head` and `list_objects` keep returning only
the current version, and `delete` removes only the current version: the
newest remaining version is promoted to current, and the key disappears once
no version remains. Older versions stay readable and deletable through
dedicated calls:

```python
page = store.list_versions("docs/a.txt", tenant="acme")
# {"items": [{"version_id", "sha256", "size", "created_at", "current"}, ...],
#  "next_after": <version_id or null>}   — newest first, paginate with after/limit
data, entry = store.get_version("docs/a.txt", version_id, tenant="acme")
entry = store.head_version("docs/a.txt", version_id, tenant="acme")
store.delete_version("docs/a.txt", version_id, tenant="acme")
```

Version reads verify the blob digest and size exactly like `get`. A malformed
`version_id` raises `ObjectStoreError("invalid version")`, an unknown key or
version raises `not found`, and calling any of these while the tenant has
versioning disabled raises `ObjectStoreError("versioning disabled")`.
Deleting a historical version never touches the current object; deleting the
current version promotes the newest remaining one.

A retention policy is saved per tenant with at least one of two non-negative
integer limits — anything else raises `ObjectStoreError("invalid retention")`:

```python
store.set_retention(max_versions=10, tenant="acme")
store.set_retention(max_age_seconds=86400, tenant="acme")
store.get_retention(tenant="acme")   # -> {"max_versions":..., "max_age_seconds":...} or None
result = store.purge_retention(dry_run=True, tenant="acme")   # preview
store.purge_retention(dry_run=False, tenant="acme")           # actually delete
```

`purge_retention` compares version `created_at` timestamps against the
current UTC time and always keeps the current version of every key; of the
remaining versions it deletes any beyond the newest `max_versions` per key or
older than `max_age_seconds`. It returns
`{"versions": [{"key","version_id","sha256","size","created_at"}, ...],
"bytes": <int>, "dry_run": <bool>}` with the removed (or, for a dry run,
removable) versions sorted by `version_id` and their total size in bytes; a
dry run changes nothing, a missing policy yields an empty result, a
non-boolean `dry_run` raises `ObjectStoreError("invalid dry_run")` and an
I/O failure while saving raises `ObjectStoreError("version io error")`.
Purged version blobs stay on disk until `collect_garbage` reclaims them —
garbage collection treats blobs referenced by any version, any current
object or any incomplete upload session as live.

The HTTP surface grows four tenant-scoped endpoints (tenant selected through
`X-Objstore-Tenant` as usual): `GET`/`PUT /v1/versioning` reads and sets the
tenant's flag with a `{"enabled": <bool>}` body (a non-boolean value returns
`400 {"error":"invalid versioning"}`); `GET /v1/objects/{key}/versions`
lists versions newest-first as `{"items": [...], "next_after": ...}` with
`after`/`limit` pagination, and `GET`/`HEAD`/`DELETE
/v1/objects/{key}/versions/{version_id}` reads, inspects or deletes one
version (GET carries `X-Content-Sha256` and `ETag` like object GET);
`GET`/`PUT /v1/retention` reads and saves the policy as
`{"max_versions":..., "max_age_seconds":...}` (invalid input returns
`400 {"error":"invalid retention"}`); and `POST /v1/retention/purge` runs
the purge with an optional `{"dry_run": <bool>}` body (default `true`), a
non-boolean value returning `400 {"error":"invalid dry_run"}`. Object `PUT`
and upload completion responses include `version_id` while the tenant has
versioning enabled. The command line interface is unchanged by this feature.

### Tenant quotas and usage

Each tenant may carry a quota policy with at least one of two non-negative
integer limits — anything else (both missing, booleans, negatives, other
types or, over HTTP, unknown fields) raises `ObjectStoreError("invalid quota")`
/ returns `400 {"error":"invalid quota"}`:

```python
store.set_quota(max_bytes=1_000_000, tenant="acme")
store.set_quota(max_objects=100, tenant="acme")
store.get_quota(tenant="acme")   # -> {"max_bytes":..., "max_objects":...} or None
store.clear_quota(tenant="acme") # back to unlimited
store.get_usage(tenant="acme")
# -> {"used_bytes": <int>, "used_objects": <int>,
#     "reserved_bytes": <int>, "reserved_sessions": <int>}
```

Usage is counted by logical reference, not physical dedup: every current
object and every retained version counts once (a current pointer duplicating
its newest version is not counted twice), keys sharing a digest count
separately, and the same key in two tenants counts in both. Active upload
sessions reserve their declared size in `reserved_bytes` (and count in
`reserved_sessions`) until they complete or abort. `max_bytes` limits used
plus reserved bytes; `max_objects` limits logical objects. A `put` or
upload completion whose resulting usage would exceed a limit fails
atomically with `ObjectStoreError("quota exceeded")` (HTTP
`413 {"error":"quota exceeded"}`) before any data lands; `begin_upload`
refuses to create a session whose reservation would exceed `max_bytes`, and
completion releases the session's own reservation before checking. Deletes,
version purges and aborts free quota; garbage collection never changes it.
The policy is persisted in the index, an I/O failure while saving it raises
`ObjectStoreError("quota io error")` (HTTP `500`), and tenants without a
saved policy see no behaviour change in any API, HTTP path or CLI command.

The HTTP surface grows two tenant-scoped endpoints (tenant selected through
`X-Objstore-Tenant` as usual): `GET`/`PUT`/`DELETE /v1/quota` reads, saves
or clears the policy as `{"max_bytes":..., "max_objects":...}` (unset
limits read as `null`), and `GET /v1/usage` returns the four counters
exactly like `get_usage`. The command line interface is unchanged by this
feature.

### Object lifecycle policies

Every object written by a new version of the store — a `put` or a completed
resumable upload — records a comparable UTC `last_modified` timestamp in the
index, persisted atomically with every other piece of metadata. The public
`put`/`get`/`head`/upload metadata shape is unchanged (the timestamp stays
internal), and indexes written by older versions simply have no such field:
reopening an old data directory never errors, and those objects are treated
as timeless. A tenant may save one lifecycle policy with an optional string
`prefix` (empty string when omitted) and a required non-negative integer
`max_age_seconds` (booleans excluded); a missing age, a bad age or a
non-string prefix raises `ObjectStoreError("invalid lifecycle")`:

```python
store.set_lifecycle(86400, prefix="tmp/", tenant="acme")
store.get_lifecycle(tenant="acme")   # -> {"prefix": "tmp/", "max_age_seconds": 86400} or None
store.clear_lifecycle(tenant="acme") # remove the policy
result = store.run_lifecycle(dry_run=True, tenant="acme")   # preview (the default)
store.run_lifecycle(dry_run=False, tenant="acme")           # actually delete
```

`run_lifecycle` computes its cutoff from the current UTC time and inspects
only the tenant's current objects whose keys start with `prefix` and whose
last-modified time is strictly older than the cutoff; timeless objects
(entries without a timestamp) never match. The candidates are sorted by key
and the call returns
`{"objects": [{"key","sha256","size"}, ...], "bytes": <int>,
"dry_run": <bool>}` with the total size of the candidates; without a saved
policy it returns `{"objects": [], "bytes": 0, "dry_run": ...}`. A non-
boolean `dry_run` raises `ObjectStoreError("invalid dry_run")`. A preview
changes no state; an actual run performs every removal under the store lock
in one atomic index rewrite. Versioned tenants follow the exact rules of
`delete`: the current version is removed and the newest remaining version is
promoted (its `created_at` becoming the key's last-modified time), while
unversioned tenants lose the current mapping. Active upload sessions are
untouched, usage counters reflect the removed references, and a save
failure raises `ObjectStoreError("lifecycle io error")` with the object
maps, versions and quota counters restored to their pre-call state.
Lifecycle removes metadata references only; the blob bytes stay on disk
until `collect_garbage` reclaims them.

The HTTP surface grows two tenant-scoped endpoints (tenant selected through
`X-Objstore-Tenant` as usual):

* `GET /v1/lifecycle` returns the saved policy as JSON or `null` when none
  is saved; `PUT /v1/lifecycle` accepts a JSON object holding only the
  optional `prefix` (defaults to `""`) and the required
  `max_age_seconds`, and returns the saved policy; `DELETE
  /v1/lifecycle` clears the policy and returns `null`. Invalid JSON,
  unknown fields, a missing or invalid age and a non-string prefix return
  `400` (`invalid json` / `invalid lifecycle`).
* `POST /v1/lifecycle/run` runs the policy. The body is optional; when
  present it must be a JSON object holding only a boolean `dry_run`
  (default `true`). Invalid JSON, unknown fields or a non-boolean value
  return `400` (`invalid json` / `invalid dry_run`). The response is the
  `{"objects","bytes","dry_run"}` result described above.

An invalid tenant on either endpoint returns `400 {"error":"invalid
tenant"}`, and a persistence failure returns `500 {"error":"lifecycle io
error"}` with nothing changed. Tenants without a policy and the command line
interface are unchanged by this feature.

### Read access policies and request signing

Each tenant may carry a read access policy, persisted atomically with the
index. The policy is `public` — the default, represented by the absence of
an entry, so indexes written by older versions keep their fully public
behaviour — or `signed` with a required 16 to 256 character UTF-8 `secret`.
Any other mode or secret raises `ObjectStoreError("invalid access policy")`:

```python
store.set_access_policy("signed", secret="0123456789abcdef", tenant="acme")
store.get_access_policy(tenant="acme")   # -> {"mode": "signed", "secret": ...} or {"mode": "public"}
store.clear_access_policy(tenant="acme") # back to public (set_access_policy("public") works too)
```

While a tenant is signed, `get`, `head`, `get_version` and `head_version`
require a matching `expires` (a future integer Unix timestamp) and
`signature` (64 lowercase hex digits), checked in constant time *before*
any key or version is looked up; anything missing, expired or mismatching
raises `ObjectStoreError("access denied")`, while a valid signature on an
unknown key or version still raises the usual `not found`. The signature
covers the tenant name, the HTTP method (`GET` or `HEAD`), the object key,
the optional version id and the expiry, joined by newlines and hashed with
HMAC-SHA256 keyed by the secret; `store.sign_request("GET", "doc",
expires=..., tenant="acme")` computes exactly that string (a bad method, a
non-integer or past expiry, or a tenant without a signed policy raises
`ObjectStoreError("invalid signature")`). Writes, deletes, listings and
every other operation are unaffected, and a persistence failure raises
`ObjectStoreError("access policy io error")` with the previous policy kept.

The HTTP surface grows one tenant-scoped endpoint (tenant selected through
`X-Objstore-Tenant` as usual): `GET /v1/access-policy` returns
`{"mode": "public"|"signed"}` (the secret is never returned), `PUT
/v1/access-policy` accepts a JSON object with `mode` and, for `signed`, a
`secret`, and `DELETE /v1/access-policy` restores `public`. Invalid JSON or
an invalid policy returns `400 {"error":"invalid access policy"}` and a
persistence failure returns `500 {"error":"access policy io error"}` with
the old policy unchanged. While a tenant is signed, object and version
`GET`/`HEAD` requests must carry `expires` and `signature` query
parameters; missing, repeated, malformed, expired or mismatching values
return `403 {"error":"access denied"}`, a valid signature on an unknown key
or version still returns `404`, and successful responses keep their `ETag`
and `X-Content-Sha256` headers.

### Tenant rate limiting

Each tenant may carry a rate limit policy, persisted atomically with the
index. The policy requires `window_seconds` (an integer between 1 and
86400, booleans excluded) plus at least one of `max_requests` and
`max_bytes` (non-negative integers, booleans excluded); anything else —
booleans, negatives, unknown fields, a missing window or no limit at all —
raises `ObjectStoreError("invalid rate limit")` / returns
`400 {"error":"invalid rate limit"}`:

```python
store.set_rate_limit(60, max_requests=100, tenant="acme")
store.set_rate_limit(3600, max_requests=1000, max_bytes=10_000_000, tenant="acme")
store.get_rate_limit(tenant="acme")
# -> {"window_seconds":..., "max_requests":..., "max_bytes":...} or None
store.clear_rate_limit(tenant="acme")   # back to unlimited
store.get_rate_limit_usage(tenant="acme")
# -> {"window_start": <int>, "reset_at": <int>,
#     "requests": <int>, "bytes": <int>} or None
```

While a policy is saved, every object, blob, upload, version, retention,
quota, lifecycle and access-policy operation on that tenant counts once
against `max_requests`, and the payload bytes of `put` and
`append_upload` (never metadata JSON) count against `max_bytes`. The
counters live in fixed UTC windows aligned to the Unix epoch: they reset
whenever the window changes, and they live in memory only, so reopening
the data directory keeps the policy but starts the current window from
zero. Tenant, framing, signature and conditional-header validation runs
first — a request failing any of those keeps its original error and is
not counted. A request that passes validation is admitted atomically
(under the store lock, so concurrent requests can never exceed a limit)
before any state changes or any response is sent; a rejected request
raises `ObjectStoreError("rate limit exceeded")` (HTTP
`429 {"error":"rate limit exceeded"}` with a `Retry-After` header holding
the seconds left in the window, always at least 1) and changes neither
storage nor counters. Only `/healthz`, the rate-limit management
endpoints and the audit endpoints are never counted. The policy is persisted in the index, an
I/O failure while saving it raises `ObjectStoreError("rate limit io
error")` (HTTP `500`) and keeps the previous policy, clearing a tenant
without a policy succeeds, and reading one without a policy returns
`None`/`null`. Tenants without a saved policy see no behaviour change in
any API, HTTP path or CLI command.

The HTTP surface grows two tenant-scoped endpoints (tenant selected
through `X-Objstore-Tenant` as usual): `GET`/`PUT`/`DELETE /v1/rate-limit`
reads, saves or clears the policy as `{"window_seconds":...,
"max_requests":..., "max_bytes":...}` (unset limits read as `null`, no
policy reads as `null`, `DELETE` returns `null`), and
`GET /v1/rate-limit/usage` returns the four counters exactly like
`get_rate_limit_usage` (`null` without a policy). The command line
interface is unchanged by this feature.

### Audit log

Every object, upload-session, version and policy operation — through the
Python API or HTTP — appends one event to `<data-dir>/audit.log` before
returning; `blob` reads and garbage collection are recorded under the
`global` tenant. An event is one JSON line holding a per-tenant ascending
`seq`, a UTC `timestamp`, the `tenant`, the `operation`, the target
`key`/`session`/`version` when applicable and the `result` (`"ok"` or
`"error"`); successful object events also carry `sha256` and `size`, and
failures a stable `error` message. Content, request bodies and secrets
are never recorded. Each tenant's events form a hash chain: every event
records the previous event's hash in `prev_hash` and its own `hash`, so
tampering is detectable per tenant.

```python
store.list_audit_events(after=None, limit=100, tenant="acme")
# -> {"items": [{"seq","timestamp","tenant","operation","result",...,
#                "prev_hash","hash"}, ...],
#     "next_after": <seq or null>}   — ascending seq, paginate with after/limit
store.verify_audit_log(tenant="acme")
# -> {"ok": True, "events": <int>, "last_seq": <int or null>,
#     "head": <hash or null>}
```

`after` is an exclusive non-negative integer sequence cursor and `limit`
bounds the page at 1–1000; tenants only ever see their own events and an
unknown tenant reads as an empty page. Querying and verifying record no
audit events themselves, and any parameter problem raises
`ObjectStoreError("invalid audit query")` (HTTP
`400 {"error":"invalid audit query"}`). Reopening the data directory
continues every sequence, silently dropping a trailing incomplete record
left by a crash; a format error, a sequence gap or a chain mismatch
anywhere else raises `ObjectStoreError("audit corrupted")` (HTTP `500`),
and a directory without a log opens as an empty one. A failed audit
append raises `ObjectStoreError("audit io error")` (HTTP `500`): write
operations roll their object, version, session and policy changes back
and read operations return no data.

The HTTP surface grows two tenant-scoped endpoints (tenant selected
through `X-Objstore-Tenant` as usual): `GET /v1/audit?after=&limit=`
returns one page of the tenant's events exactly like
`list_audit_events`, and `GET /v1/audit/verify` returns the verification
summary exactly like `verify_audit_log`. Neither endpoint records an
audit event or consumes rate-limit budget, and the command line
interface is unchanged by this feature.


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
| PUT | `/v1/objects/{key}` | 201 `{"key","sha256","size"}` on first store; 200 with the same body when the same bytes are stored again | 400 invalid key / invalid precondition, 411 missing Content-Length, 405, 412 precondition failed, 413 quota exceeded, 500 blob io error |
| GET | `/v1/objects/{key}` | 200 raw bytes, headers `Content-Type`, `X-Content-Sha256` and `ETag` (`"<sha256>"`) | 404 unknown key, 400 invalid key, 403 access denied (signed tenant), 405, 500 corrupted blob / blob io error |
| HEAD | `/v1/objects/{key}` | 200 no body, headers `Content-Length`, `X-Content-Sha256` and `ETag` | 404 unknown key, 400 invalid key, 403 access denied (signed tenant), 405 |
| DELETE | `/v1/objects/{key}` | 204 no body | 404 unknown key, 400 invalid key / invalid precondition, 405, 412 precondition failed |
| GET | `/v1/objects?prefix=&after=&limit=` | 200 `{"items":[{"key","sha256","size","content_type"}],"next_after":<str or null>}` | 400 invalid limit, 400 invalid prefix, 400 invalid after, 405 |
| GET | `/v1/blobs/{sha256}` | 200 raw bytes of that content, header `X-Content-Sha256` | 400 invalid sha256, 404 unknown digest, 405, 500 corrupted blob / blob io error |
| POST | `/v1/uploads` | 201 `{"session":"<id>"}`; body is a UTF-8 JSON object with required `key`, `size`, `sha256` and optional `content_type` (default `null`); optional `If-Match`/`If-None-Match` save a publish-time condition | 400 invalid json / fields / invalid precondition, 411 missing/non-integer/negative Content-Length, 400 Transfer-Encoding / truncated body, 405, 413 quota exceeded, 500 upload io error |
| GET | `/v1/uploads/{session}` | 200 `{"key","size","sha256","content_type","offset","completed"}` | 404 unknown or aborted session, 405 |
| PUT | `/v1/uploads/{session}?offset=<n>` | 200 `{"offset":<confirmed length>}`; body holds the raw chunk bytes and `offset` must be a unique non-negative decimal integer | 400 invalid/truncated framing, 404 unknown or aborted session, 409 gap/overlap/overflow/conflict/empty-chunk/completed, 411 missing/non-integer/negative Content-Length, 405, 500 upload io error |
| POST | `/v1/uploads/{session}/complete` | 200 `{"sha256","size","content_type"}` (the first result on repeat completion); no body required | 404 unknown or aborted session, 409 incomplete or hash mismatch, 412 saved precondition failed, 413 quota exceeded, 400 bad framing, 405, 500 blob/upload io error |
| DELETE | `/v1/uploads/{session}` | 204 no body; aborts the session and keeps any published object | 404 unknown or aborted session, 405 |
| GET | `/v1/versioning` | 200 `{"enabled": <bool>}` for the selected tenant | 400 invalid tenant, 405 |
| PUT | `/v1/versioning` | 200 `{"enabled": <bool>}`; body is a JSON object with a boolean `enabled` | 400 invalid json / invalid versioning, 405, 411 |
| GET | `/v1/objects/{key}/versions?after=&limit=` | 200 `{"items":[{"version_id","sha256","size","created_at","current"}],"next_after":<str or null>}`, newest first | 400 invalid key / invalid limit / invalid version / versioning disabled, 404 unknown key, 405 |
| GET | `/v1/objects/{key}/versions/{version_id}` | 200 raw bytes of that version, headers `Content-Type`, `X-Content-Sha256` and `ETag` | 400 invalid key / invalid version / versioning disabled, 403 access denied (signed tenant), 404 unknown key or version, 405, 500 corrupted blob / blob io error |
| HEAD | `/v1/objects/{key}/versions/{version_id}` | 200 no body, headers `Content-Length`, `X-Content-Sha256` and `ETag` | 400 invalid key / invalid version / versioning disabled, 403 access denied (signed tenant), 404 unknown key or version, 405 |
| DELETE | `/v1/objects/{key}/versions/{version_id}` | 204 no body; deleting the current version promotes the newest remaining one | 400 invalid key / invalid version / versioning disabled, 404 unknown key or version, 405 |
| GET | `/v1/retention` | 200 `{"max_versions": <int|null>, "max_age_seconds": <int|null>}` | 400 invalid tenant, 405 |
| PUT | `/v1/retention` | 200 with the saved policy; body holds at least one of the two non-negative integer limits | 400 invalid json / invalid retention, 405, 411 |
| POST | `/v1/retention/purge` | 200 `{"versions":[...],"bytes":<int>,"dry_run":<bool>}`; optional body `{"dry_run": <bool>}` (default `true`) | 400 invalid json / invalid dry_run, 405, 500 version io error |
| GET | `/v1/quota` | 200 `{"max_bytes": <int|null>, "max_objects": <int|null>}` for the selected tenant | 400 invalid tenant, 405 |
| PUT | `/v1/quota` | 200 with the saved policy; body holds at least one of the two non-negative integer limits and no unknown fields | 400 invalid json / invalid quota, 405, 411, 500 quota io error |
| DELETE | `/v1/quota` | 200 `{"max_bytes": null, "max_objects": null}`; clears the policy | 400 invalid tenant, 405, 500 quota io error |
| GET | `/v1/usage` | 200 `{"used_bytes","used_objects","reserved_bytes","reserved_sessions"}` for the selected tenant | 400 invalid tenant, 405 |
| GET | `/v1/lifecycle` | 200 `{"prefix": <str>, "max_age_seconds": <int>}` or `null` for the selected tenant | 400 invalid tenant, 405 |
| PUT | `/v1/lifecycle` | 200 with the saved policy; body is a JSON object with required non-negative integer `max_age_seconds`, optional string `prefix` (default `""`) and no other fields | 400 invalid json / invalid lifecycle, 405, 411, 500 lifecycle io error |
| DELETE | `/v1/lifecycle` | 200 `null`; clears the policy (idempotent) | 400 invalid tenant, 405, 500 lifecycle io error |
| POST | `/v1/lifecycle/run` | 200 `{"objects":[{"key","sha256","size"}],"bytes":<int>,"dry_run":<bool>}`; optional body `{"dry_run": <bool>}` (default `true`); a real run removes expired current references under one atomic rewrite | 400 invalid json / invalid dry_run / invalid tenant, 405, 500 lifecycle io error |
| GET | `/v1/access-policy` | 200 `{"mode": "public"|"signed"}` for the selected tenant | 400 invalid tenant, 405 |
| PUT | `/v1/access-policy` | 200 `{"mode": ...}`; body is a JSON object with `mode` (`public`/`signed`) and, for `signed`, a 16–256 character UTF-8 `secret` | 400 invalid access policy, 405, 411, 500 access policy io error |
| DELETE | `/v1/access-policy` | 200 `{"mode": "public"}`; restores public reads | 400 invalid tenant, 405, 500 access policy io error |
| GET | `/v1/rate-limit` | 200 `{"window_seconds": <int>, "max_requests": <int|null>, "max_bytes": <int|null>}` or `null` for the selected tenant | 400 invalid tenant, 405 |
| PUT | `/v1/rate-limit` | 200 with the saved policy; body is a JSON object with required `window_seconds` (1–86400) and at least one of `max_requests`/`max_bytes` (non-negative integers), no unknown fields | 400 invalid rate limit, 405, 411, 500 rate limit io error |
| DELETE | `/v1/rate-limit` | 200 `null`; clears the policy (idempotent) | 400 invalid tenant, 405, 500 rate limit io error |
| GET | `/v1/rate-limit/usage` | 200 `{"window_start","reset_at","requests","bytes"}` or `null` for the selected tenant | 400 invalid tenant, 405 |
| GET | `/v1/audit?after=&limit=` | 200 `{"items":[{...event...}],"next_after":<int or null>}` for the selected tenant | 400 invalid audit query / invalid tenant, 405, 500 audit corrupted / audit io error |
| GET | `/v1/audit/verify` | 200 `{"ok":true,"events":<int>,"last_seq":<int or null>,"head":<str or null>}` for the selected tenant | 400 invalid audit query / invalid tenant, 405, 500 audit corrupted / audit io error |

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
  index.json            # {"version":1,"objects":{"<key>":{"sha256","size","content_type",
                        #   "last_modified"?}},
                        #  "tenants":{"<tenant>":{"<key>":{...}}} (non-default tenants,
                        #  only while non-empty),
                        #  "uploads":{"<session>":{"key","entry","tenant"?,"version_id"?}}
                        #  (only while non-empty),
                        #  "versioning":{"<tenant>":true} (only enabled tenants),
                        #  "versions":{"<tenant>":{"<key>":[{"version_id","sha256","size",
                        #  "content_type","created_at"}, ...]}} (only while non-empty),
                        #  "retention":{"<tenant>":{"max_versions"?,"max_age_seconds"?}},
                        #  "quota":{"<tenant>":{"max_bytes"?,"max_objects"?}},
                        #  "lifecycle":{"<tenant>":{"prefix","max_age_seconds"}},
                        #  "access":{"<tenant>":{"mode":"signed","secret"}}
                        #  (only tenants with a saved policy),
                        #  "rate_limit":{"<tenant>":{"window_seconds",
                        #  "max_requests"?,"max_bytes"?}} (only tenants with
                        #  a saved policy)}
  blobs/<sha256>        # raw object bytes, one file per distinct digest
  audit.log             # one JSON event per line: {"seq","timestamp","tenant",
                        # "operation","result","key"?,"session"?,"version"?,
                        # "sha256"?,"size"?,"error"?,"prev_hash","hash"}
                        # (created on the first audited operation)
  uploads/<session>.json  # active upload declaration plus confirmed offset, tenant
                          # (non-default only) and optional expected_sha256 publish
                          # condition (lazy directory)
  uploads/<session>.part  # received bytes of an active upload
```

The flat `objects` map holds the `default` tenant, so indexes written before
tenants existed need no migration; the same applies to session files and
completion records without a `tenant` field. Indexes written before
versioning existed have no `versioning`, `versions` or `retention` maps, so
every tenant starts with versioning disabled and no key gains versions it
never wrote. Indexes written before quotas existed have no `quota` map, so
every tenant starts unlimited. Indexes written before lifecycle existed have
no `lifecycle` map and their entries lack `last_modified`; those directories
open unchanged and the timeless entries are never expired by a lifecycle
run. Indexes written before access policies existed have no `access` map, so
every tenant starts public. Indexes written before rate limiting existed
have no `rate_limit` map, so every tenant starts unlimited. Directories
written before auditing existed have no `audit.log`; they open as an
empty log and the file appears with the first audited operation.

## Limits of this seed

The following long-term goals are intentionally not implemented yet:
consistent hashing and rebalancing, erasure coding and
repair, cross-region replication and end-to-end audit logging. Resumable
uploads are exposed through the
Python API and the HTTP surface; the command line interface does not
expose them yet, and likewise exposes no versioning, retention, quota,
lifecycle, access-policy or rate-limit commands.
Authentication is out of scope: the server trusts every caller.
