"""Panetone's local control protocol, durable idempotency, and Unix server."""

import asyncio
import hashlib
import json
import os
import socket
import sqlite3
import stat
import struct
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

SCHEMA = "panetone.control.v1"
MAX_REQUEST_BYTES = 256 * 1024
JOURNAL_MAX_PAGES = 16_384  # 64 MiB with the fixed 4 KiB page size.
COMPLETED_RETENTION_SECONDS = 30 * 24 * 60 * 60
NONTERMINAL_STATES = {"in_progress", "audit_posted", "delivering"}
TERMINAL_STATES = {"succeeded", "failed", "indeterminate"}


def default_socket_path():
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}"
    return Path(runtime_dir) / "panetone" / "control.sock"


def make_response(request_id, *, result=None, error=None):
    response = {
        "schema": SCHEMA,
        "id": request_id,
        "ok": error is None,
    }
    if error is None:
        response["result"] = result or {}
    else:
        response["error"] = error
    return response


def make_error(request_id, code, message, details=None):
    error = {"code": code, "message": message}
    if details:
        error["details"] = details
    return make_response(request_id, error=error)


class ProtocolError(Exception):
    def __init__(self, code, message, request_id=None, details=None):
        super().__init__(message)
        self.code = code
        self.request_id = request_id
        self.details = details


class RequestFailure(Exception):
    """A handled request failure that must be persisted and returned."""

    def __init__(self, code, message, *, details=None, indeterminate=False):
        super().__init__(message)
        self.code = code
        self.details = details
        self.indeterminate = indeterminate


def parse_request(data):
    try:
        request = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProtocolError(
            "invalid_json", "request must be valid UTF-8 JSON"
        ) from error
    if not isinstance(request, dict):
        raise ProtocolError("invalid_request", "request must be a JSON object")

    request_id = request.get("id")
    if not isinstance(request_id, str):
        raise ProtocolError("invalid_id", "id must be a UUID string")
    try:
        request_id = str(uuid.UUID(request_id))
    except ValueError as error:
        raise ProtocolError("invalid_id", "id must be a UUID string") from error

    if request.get("schema") != SCHEMA:
        raise ProtocolError(
            "unsupported_schema",
            f"schema must be {SCHEMA}",
            request_id,
        )
    if request.get("method") != "send":
        raise ProtocolError("unknown_method", "method must be send", request_id)
    params = request.get("params")
    if not isinstance(params, dict):
        raise ProtocolError(
            "invalid_params", "params must be a JSON object", request_id
        )
    source = params.get("from")
    target = params.get("to")
    message = params.get("message")
    for name, value in (("from", source), ("to", target), ("message", message)):
        if not isinstance(value, str) or not value:
            raise ProtocolError(
                "invalid_params",
                f"params.{name} must be a non-empty string",
                request_id,
            )
    if source != source.strip() or target != target.strip():
        raise ProtocolError(
            "invalid_params",
            "route names must not have leading or trailing whitespace",
            request_id,
        )
    has_control = any(
        ord(char) < 0x20 or 0x7F <= ord(char) <= 0x9F
        for value in (source, target)
        for char in value
    )
    if has_control:
        raise ProtocolError(
            "invalid_params",
            "route names must not contain control characters",
            request_id,
        )
    if len(source) > 128 or len(target) > 128:
        raise ProtocolError(
            "invalid_params", "route names must be at most 128 characters", request_id
        )

    return_final = params.get("return_final", False)
    if not isinstance(return_final, bool):
        raise ProtocolError(
            "invalid_params", "params.return_final must be a boolean", request_id
        )
    timeout_ms = params.get("timeout_ms", 0)
    if not isinstance(timeout_ms, int) or isinstance(timeout_ms, bool) or timeout_ms < 0:
        raise ProtocolError(
            "invalid_params", "params.timeout_ms must be a non-negative integer", request_id
        )
    if timeout_ms and not return_final:
        raise ProtocolError(
            "invalid_params", "params.timeout_ms requires return_final", request_id
        )

    normalized_params = {"from": source, "to": target, "message": message}
    if "return_final" in params:
        normalized_params["return_final"] = return_final
    if "timeout_ms" in params:
        normalized_params["timeout_ms"] = timeout_ms
    return {
        "schema": SCHEMA,
        "id": request_id,
        "method": "send",
        "params": normalized_params,
    }


def request_hash(request):
    content = {
        "schema": request["schema"],
        "method": request["method"],
        "params": request["params"],
    }
    encoded = json.dumps(
        content, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class ControlJournal:
    def __init__(self, path):
        self.path = Path(path)

    def initialize(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.is_symlink():
            raise RuntimeError(f"control journal must not be a symlink: {self.path}")
        flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(self.path, flags, 0o600)
        try:
            os.fchmod(fd, 0o600)
        finally:
            os.close(fd)
        with self._connect() as db:
            db.execute("PRAGMA page_size=4096")
            db.execute(f"PRAGMA max_page_count={JOURNAL_MAX_PAGES}")
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS control_request (
                    request_id TEXT PRIMARY KEY,
                    request_hash TEXT NOT NULL,
                    source TEXT NOT NULL,
                    target TEXT NOT NULL,
                    state TEXT NOT NULL,
                    progress_json TEXT,
                    response_json TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS return_delivery (
                    request_id TEXT PRIMARY KEY REFERENCES control_request(request_id),
                    source_json TEXT NOT NULL,
                    target_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    result_json TEXT,
                    agent_state TEXT NOT NULL,
                    telegram_state TEXT NOT NULL,
                    last_error TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS control_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            db.execute(
                """
                UPDATE return_delivery SET
                    agent_state = CASE WHEN agent_state = 'delivering' THEN 'indeterminate' ELSE agent_state END,
                    telegram_state = CASE WHEN telegram_state = 'delivering' THEN 'indeterminate' ELSE telegram_state END,
                    last_error = CASE
                        WHEN agent_state = 'delivering' OR telegram_state = 'delivering'
                        THEN 'Panetone restarted during callback delivery; destination receipt is indeterminate'
                        ELSE last_error
                    END,
                    updated_at = ?
                WHERE agent_state = 'delivering' OR telegram_state = 'delivering'
                """,
                (time.time(),),
            )
        self.prune_completed()
        for suffix in ("-wal", "-shm"):
            sidecar = Path(f"{self.path}{suffix}")
            if sidecar.exists():
                os.chmod(sidecar, 0o600)

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        db.execute(f"PRAGMA max_page_count={JOURNAL_MAX_PAGES}")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA wal_autocheckpoint=100")
        db.execute("PRAGMA journal_size_limit=4194304")
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def get(self, request_id):
        with self._connect() as db:
            return db.execute(
                "SELECT * FROM control_request WHERE request_id = ?", (request_id,)
            ).fetchone()

    def prune_completed(self, now=None):
        cutoff = (time.time() if now is None else now) - COMPLETED_RETENTION_SECONDS
        with self._connect() as db:
            db.execute(
                """
                DELETE FROM return_delivery
                WHERE request_id IN (
                    SELECT request_id FROM control_request
                    WHERE state IN ('succeeded', 'failed') AND updated_at < ?
                )
                  AND agent_state = 'delivered'
                  AND telegram_state = 'delivered'
                """,
                (cutoff,),
            )
            return db.execute(
                """
                DELETE FROM control_request
                WHERE state IN ('succeeded', 'failed') AND updated_at < ?
                  AND request_id NOT IN (SELECT request_id FROM return_delivery)
                """,
                (cutoff,),
            ).rowcount

    def claim(self, request, digest):
        now = time.time()
        params = request["params"]
        try:
            with self._connect() as db:
                db.execute(
                    """
                    INSERT INTO control_request (
                        request_id, request_hash, source, target, state,
                        progress_json, response_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'in_progress', NULL, NULL, ?, ?)
                    """,
                    (request["id"], digest, params["from"], params["to"], now, now),
                )
            return True
        except sqlite3.IntegrityError:
            return False
        except sqlite3.OperationalError as error:
            if "full" in str(error).casefold():
                raise RequestFailure(
                    "journal_full",
                    "the durable control journal is full; no delivery was attempted",
                ) from error
            raise

    def transition(self, request_id, state, progress=None):
        if state not in NONTERMINAL_STATES:
            raise ValueError(f"invalid nonterminal state: {state}")
        progress_json = (
            json.dumps(progress, ensure_ascii=False, separators=(",", ":"))
            if progress is not None
            else None
        )
        with self._connect() as db:
            changed = db.execute(
                """
                UPDATE control_request
                SET state = ?, progress_json = ?, updated_at = ?
                WHERE request_id = ? AND response_json IS NULL
                """,
                (state, progress_json, time.time(), request_id),
            ).rowcount
        if changed != 1:
            raise RuntimeError(f"cannot transition completed request {request_id}")

    def finish(self, request_id, state, response):
        if state not in TERMINAL_STATES:
            raise ValueError(f"invalid terminal state: {state}")
        encoded = json.dumps(response, ensure_ascii=False, separators=(",", ":"))
        with self._connect() as db:
            changed = db.execute(
                """
                UPDATE control_request
                SET state = ?, response_json = ?, updated_at = ?
                WHERE request_id = ? AND response_json IS NULL
                """,
                (state, encoded, time.time(), request_id),
            ).rowcount
        if changed != 1:
            row = self.get(request_id)
            if not row or row["response_json"] != encoded:
                raise RuntimeError(f"cannot finish request {request_id}")

    def register_return_route(self, request_id, source, target):
        now = time.time()
        source_json = json.dumps(source, ensure_ascii=False, separators=(",", ":"))
        target_json = json.dumps(target, ensure_ascii=False, separators=(",", ":"))
        with self._connect() as db:
            db.execute(
                """
                INSERT INTO return_delivery (
                    request_id, source_json, target_json, state, result_json,
                    agent_state, telegram_state, last_error, created_at, updated_at
                ) VALUES (?, ?, ?, 'pending', NULL, 'pending', 'pending', NULL, ?, ?)
                ON CONFLICT(request_id) DO NOTHING
                """,
                (request_id, source_json, target_json, now, now),
            )
            row = db.execute(
                "SELECT source_json, target_json FROM return_delivery WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if not row or row["source_json"] != source_json or row["target_json"] != target_json:
                raise RuntimeError(f"return route conflict for request {request_id}")

    def get_return(self, request_id):
        with self._connect() as db:
            return db.execute(
                "SELECT * FROM return_delivery WHERE request_id = ?", (request_id,)
            ).fetchone()

    def discard_unsubmitted_return(self, request_id):
        with self._connect() as db:
            changed = db.execute(
                """
                DELETE FROM return_delivery
                WHERE request_id = ? AND state = 'pending' AND result_json IS NULL
                  AND agent_state = 'pending' AND telegram_state = 'pending'
                """,
                (request_id,),
            ).rowcount
        if changed != 1:
            raise RuntimeError(
                f"cannot discard submitted or partially delivered return {request_id}"
            )

    def record_return_result(self, request_id, result):
        encoded = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
        with self._connect() as db:
            changed = db.execute(
                """
                UPDATE return_delivery SET state = 'terminal', result_json = ?, updated_at = ?
                WHERE request_id = ? AND result_json IS NULL
                """,
                (encoded, time.time(), request_id),
            ).rowcount
            if changed == 0:
                row = db.execute(
                    "SELECT result_json FROM return_delivery WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                if row and row["result_json"] != encoded:
                    raise RuntimeError(f"conflicting terminal result for request {request_id}")
        return self.get_return(request_id)

    def pending_returns(self):
        with self._connect() as db:
            return db.execute(
                """
                SELECT * FROM return_delivery
                WHERE result_json IS NOT NULL
                  AND (agent_state = 'pending' OR telegram_state = 'pending')
                ORDER BY created_at
                """
            ).fetchall()

    def set_return_destination(self, request_id, destination, state, error=None):
        if destination not in {"agent", "telegram"}:
            raise ValueError("invalid return destination")
        if state not in {
            "pending",
            "delivering",
            "delivered",
            "failed",
            "indeterminate",
        }:
            raise ValueError("invalid return destination state")
        column = f"{destination}_state"
        with self._connect() as db:
            changed = db.execute(
                f"""
                UPDATE return_delivery SET {column} = ?, last_error = ?, updated_at = ?
                WHERE request_id = ?
                """,
                (state, error, time.time(), request_id),
            ).rowcount
        if changed != 1:
            raise RuntimeError(f"no return route for request {request_id}")

    def event_cursor(self):
        with self._connect() as db:
            row = db.execute(
                "SELECT value FROM control_meta WHERE key = 'wakterm_event_cursor'"
            ).fetchone()
        return int(row["value"]) if row else 0

    def set_event_cursor(self, sequence):
        with self._connect() as db:
            db.execute(
                """
                INSERT INTO control_meta(key, value) VALUES ('wakterm_event_cursor', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(sequence),),
            )


class DurableDispatcher:
    def __init__(self, journal, handler):
        self.journal = journal
        self.handler = handler
        self._claim_lock = asyncio.Lock()
        self._inflight = {}

    async def dispatch(self, request):
        request_id = request["id"]
        digest = request_hash(request)
        wait_for = None
        owner = False

        async with self._claim_lock:
            self.journal.prune_completed()
            row = self.journal.get(request_id)
            if row:
                if row["request_hash"] != digest:
                    return make_error(
                        request_id,
                        "idempotency_conflict",
                        "this request id was already used with different content",
                        {"state": row["state"]},
                    )
                if row["response_json"]:
                    return json.loads(row["response_json"])
                wait_for = self._inflight.get(request_id)
                if wait_for is None:
                    progress = (
                        json.loads(row["progress_json"])
                        if row["progress_json"]
                        else None
                    )
                    response = make_error(
                        request_id,
                        "request_indeterminate",
                        "the service restarted or lost contact during this request; it will not be retried automatically",
                        {"last_state": row["state"], "progress": progress},
                    )
                    self.journal.finish(request_id, "indeterminate", response)
                    return response
            else:
                try:
                    owner = self.journal.claim(request, digest)
                except RequestFailure as error:
                    return make_error(request_id, error.code, str(error), error.details)
                if not owner:
                    raise RuntimeError(
                        "request claim raced without a visible journal row"
                    )
                wait_for = asyncio.get_running_loop().create_future()
                self._inflight[request_id] = wait_for

        if not owner:
            return await asyncio.shield(wait_for)

        async def transition(state, progress=None):
            self.journal.transition(request_id, state, progress)

        try:
            result = await self.handler(request, transition)
            response = make_response(request_id, result=result)
            terminal_state = "succeeded"
        except RequestFailure as error:
            response = make_error(request_id, error.code, str(error), error.details)
            terminal_state = "indeterminate" if error.indeterminate else "failed"
        except asyncio.CancelledError:
            response = make_error(
                request_id,
                "request_indeterminate",
                "request handling was interrupted; it will not be retried automatically",
            )
            self.journal.finish(request_id, "indeterminate", response)
            async with self._claim_lock:
                future = self._inflight.pop(request_id, None)
                if future and not future.done():
                    future.set_result(response)
            raise
        except Exception as error:
            response = make_error(
                request_id,
                "internal_error",
                "request handling failed unexpectedly and will not be retried automatically",
                {"error": str(error)},
            )
            terminal_state = "indeterminate"

        self.journal.finish(request_id, terminal_state, response)
        async with self._claim_lock:
            future = self._inflight.pop(request_id, None)
            if future and not future.done():
                future.set_result(response)
        return response


class ControlServer:
    def __init__(self, path, dispatcher):
        self.path = Path(path)
        self.dispatcher = dispatcher
        self.server = None
        self._bound_identity = None

    async def start(self):
        self._prepare_directory()
        await self._remove_stale_socket()
        self.server = await asyncio.start_unix_server(
            self._handle_client,
            path=self.path,
            limit=MAX_REQUEST_BYTES + 1,
        )
        os.chmod(self.path, 0o600)
        st = os.lstat(self.path)
        self._bound_identity = (st.st_dev, st.st_ino)

    def _prepare_directory(self):
        parent = self.path.parent
        if parent.exists() and (parent.is_symlink() or not parent.is_dir()):
            raise RuntimeError(f"control socket parent is not a directory: {parent}")
        parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        st = os.stat(parent)
        if st.st_uid != os.getuid():
            raise RuntimeError(
                f"control socket directory is not owned by this user: {parent}"
            )
        os.chmod(parent, 0o700)

    async def _remove_stale_socket(self):
        try:
            st = os.lstat(self.path)
        except FileNotFoundError:
            return
        if not stat.S_ISSOCK(st.st_mode):
            raise RuntimeError(
                f"refusing to replace non-socket control path: {self.path}"
            )
        identity = (st.st_dev, st.st_ino)
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(self.path), timeout=0.5
            )
        except (ConnectionRefusedError, FileNotFoundError):
            try:
                current = os.lstat(self.path)
            except FileNotFoundError:
                return
            if (current.st_dev, current.st_ino) != identity:
                raise RuntimeError("control socket changed while checking staleness")
            self.path.unlink()
            return
        except asyncio.TimeoutError as error:
            raise RuntimeError(
                f"control socket is already active: {self.path}"
            ) from error
        else:
            del reader
            writer.close()
            await writer.wait_closed()
            raise RuntimeError(f"control socket is already active: {self.path}")

    async def close(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        if self._bound_identity:
            try:
                st = os.lstat(self.path)
                if (st.st_dev, st.st_ino) == self._bound_identity:
                    self.path.unlink()
            except FileNotFoundError:
                pass
            finally:
                self._bound_identity = None

    async def _handle_client(self, reader, writer):
        request_id = None
        try:
            peer_socket = writer.get_extra_info("socket")
            if peer_socket and hasattr(socket, "SO_PEERCRED"):
                raw = peer_socket.getsockopt(
                    socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")
                )
                _pid, uid, _gid = struct.unpack("3i", raw)
                if uid != os.getuid():
                    raise ProtocolError(
                        "permission_denied",
                        "control client must use the service account",
                    )
            try:
                data = await asyncio.wait_for(reader.readline(), timeout=10)
            except ValueError as error:
                raise ProtocolError(
                    "request_too_large", "request exceeds 256 KiB"
                ) from error
            if not data:
                writer.close()
                await writer.wait_closed()
                return
            if len(data) > MAX_REQUEST_BYTES:
                raise ProtocolError("request_too_large", "request exceeds 256 KiB")
            if not data.endswith(b"\n"):
                raise ProtocolError(
                    "invalid_framing", "request must end with a newline"
                )
            request = parse_request(data[:-1])
            request_id = request["id"]
            response = await self.dispatcher.dispatch(request)
        except ProtocolError as error:
            response = make_error(
                error.request_id or request_id,
                error.code,
                str(error),
                error.details,
            )
        except asyncio.TimeoutError:
            response = make_error(
                request_id, "request_timeout", "request read timed out"
            )
        except Exception as error:
            response = make_error(
                request_id,
                "internal_error",
                "control server error",
                {"error": str(error)},
            )
        try:
            writer.write(
                json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode()
                + b"\n"
            )
            await writer.drain()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (BrokenPipeError, ConnectionResetError):
                pass
