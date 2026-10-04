"""Command line interface.

``python3 -m objstore --data-dir DIR <serve|put|get|list|delete|gc>``. Each command
prints one line of JSON on stdout and exits 0 on success, or one line of JSON on
stderr and exits non-zero on failure.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys

from .http_app import create_server
from .store import DEFAULT_LIMIT, ContentAddressedStore, ObjectStoreError

__all__ = ["build_parser", "main"]

DEFAULT_DATA_DIR = "./objstore_data"
EXIT_FAILURE = 2


class _Parser(argparse.ArgumentParser):
    """ArgumentParser that reports usage errors as one line of JSON."""

    def error(self, message):
        _fail(message)
        raise SystemExit(EXIT_FAILURE)


def build_parser():
    parser = _Parser(prog="objstore",
                     description="Content-addressed object storage backend.")
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR,
                        help="directory holding index.json and blobs (default: %(default)s)")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run the HTTP API until SIGINT/SIGTERM")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)

    put = sub.add_parser("put", help="store a local file under a key")
    put.add_argument("--key", required=True)
    put.add_argument("--file", required=True)
    put.add_argument("--content-type", default=None)

    get = sub.add_parser("get", help="write a stored object to a local file")
    get.add_argument("--key", required=True)
    get.add_argument("--out", required=True)

    listing = sub.add_parser("list", help="list object keys in lexicographic order")
    listing.add_argument("--prefix", default="")
    listing.add_argument("--after", default=None)
    listing.add_argument("--limit", type=int, default=DEFAULT_LIMIT)

    delete = sub.add_parser("delete", help="delete a key")
    delete.add_argument("--key", required=True)

    gc = sub.add_parser("gc", help="garbage-collect unreferenced blobs (preview unless --execute)")
    gc.add_argument("--execute", action="store_true",
                    help="actually delete the candidates instead of previewing them")
    return parser


def _emit(payload):
    sys.stdout.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def _fail(message):
    sys.stderr.write(json.dumps({"ok": False, "error": message},
                                sort_keys=True, separators=(",", ":")) + "\n")
    sys.stderr.flush()
    return EXIT_FAILURE


def _interrupt(signum, frame):
    raise KeyboardInterrupt


def _cmd_serve(args, store):
    httpd = create_server(store, args.host, args.port)
    host, port = httpd.server_address[0], httpd.server_address[1]
    _emit({"ok": True, "addr": "%s:%d" % (host, port), "data_dir": store.root})
    for name in ("SIGINT", "SIGTERM"):
        signum = getattr(signal, name, None)
        try:
            signal.signal(signum, _interrupt)
        except (TypeError, ValueError, OSError):
            pass
    try:
        httpd.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


def _cmd_put(args, store):
    with open(args.file, "rb") as handle:
        payload = handle.read()
    entry = store.put(args.key, payload, content_type=args.content_type)
    _emit({"ok": True, "key": args.key, "sha256": entry["sha256"], "size": entry["size"]})
    return 0


def _cmd_get(args, store):
    data, entry = store.get(args.key)
    directory = os.path.dirname(os.path.abspath(args.out))
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(args.out, "wb") as handle:
        handle.write(data)
    _emit({"ok": True, "key": args.key, "sha256": entry["sha256"], "size": entry["size"],
           "out": os.path.abspath(args.out)})
    return 0


def _cmd_list(args, store):
    result = store.list_objects(prefix=args.prefix, after=args.after, limit=args.limit)
    _emit({"ok": True, "items": result["items"], "next_after": result["next_after"]})
    return 0


def _cmd_delete(args, store):
    store.delete(args.key)
    _emit({"ok": True, "key": args.key, "deleted": True})
    return 0


def _cmd_gc(args, store):
    result = store.collect_garbage(dry_run=not args.execute)
    _emit({"ok": True, "digests": result["digests"], "bytes": result["bytes"],
           "dry_run": result["dry_run"]})
    return 0


_HANDLERS = {"serve": _cmd_serve, "put": _cmd_put, "get": _cmd_get,
             "list": _cmd_list, "delete": _cmd_delete, "gc": _cmd_gc}


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        store = ContentAddressedStore(args.data_dir)
        return _HANDLERS[args.command](args, store)
    except ObjectStoreError as exc:
        return _fail(str(exc))
    except OSError as exc:
        return _fail("io error: %s" % (exc,))
