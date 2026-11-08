#!/usr/bin/env python3
"""Opt-in Linux runtime check against the checksum-pinned official nightly.

Example, from the repository root:
  python3 native/t3-code-ls/tools/verify-upstream.py \
    --archive /tmp/t3-0.1.8-nightly.20261108.4367-linux-x64.tar.gz \
    --output /tmp/t3-code-ls-upstream-verification.json

This runs real processes and local sockets; unit-test discovery must not run it.
Supply the official release archive. No provider turn, real user state, browser,
public relay, or public endpoint discovery is used. All application state and
identity keys are temporary. The JSON report contains no pairing tokens/cookies.
The pinned fixture must be reviewed when upstream.lock.json advances.
"""

from __future__ import annotations

import argparse
import base64
from collections import deque
from concurrent.futures import Future, TimeoutError as FutureTimeout
from contextlib import ExitStack
import hashlib
import http.client
from http.cookies import SimpleCookie
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tarfile
import tempfile
from threading import Thread
from typing import Any, Callable
from urllib.parse import urlsplit


VERSION = "0.1.8-nightly.20261108.4367"
TAG = "v" + VERSION
COMMIT = "6417151c093e54dbba0e6c824c69d03983793b9a"
ARCHIVE_SHA256 = "c60a3b994db200626912f2be776b2bae8d36a013a5e66eebf19c2671e4fe3238"
RELEASE_URL = "https://github.com/pingdotgg/t3code/releases/tag/" + TAG
BUNDLE_NAME = "t3-" + VERSION + "-linux-x64"
READ_SCOPE = "orchestration:read"
SOCKET_TIMEOUT = 15
MAX_BODY = 8 * 1024 * 1024
WORKSPACE = Path(__file__).resolve().parent.parent


class VerificationError(Exception):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise VerificationError(message)


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def redacted(line: str) -> str:
    """Startup also prints a QR code; omitting token lines alone is insufficient."""
    line = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", line).strip()
    if any(char in line for char in "█▀▄") or re.search(
        r"(?i)token|pairing|app link|cookie|credential|authorization", line
    ):
        return "[credential-bearing output omitted]"
    return re.sub(r"(?:https?|t3code)://\S+", "[url omitted]", line)[:500]


class Process:
    """Wait for a stdout event, then keep draining output until exact-PID cleanup."""

    def __init__(
        self, name: str, args: list[str], cwd: Path, env: dict[str, str],
        ready: Callable[[str], Any | None],
    ):
        self.name = name
        self.lines = 0
        self.tail: deque[str] = deque(maxlen=8)
        self.last_startup_phase: str | None = None
        self.ready: Future[Any] = Future()
        self.forced_shutdown = False
        self.process = subprocess.Popen(
            args, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            encoding="utf-8", errors="replace", bufsize=1,
        )

        def drain() -> None:
            assert self.process.stdout is not None
            try:
                for line in self.process.stdout:
                    self.lines += 1
                    self.tail.append(redacted(line))
                    if "startup phase:" in line:
                        self.last_startup_phase = redacted(line)
                    if not self.ready.done():
                        value = ready(line.strip())
                        if value is not None:
                            self.ready.set_result(value)
                if not self.ready.done():
                    self.ready.set_exception(VerificationError(
                        f"{name} exited before its readiness event: " + " | ".join(self.tail)
                    ))
            except Exception as error:
                if not self.ready.done():
                    self.ready.set_exception(error)

        self.reader = Thread(target=drain, daemon=True)
        self.reader.start()

    def await_ready(self) -> Any:
        try:
            return self.ready.result(timeout=60)
        except FutureTimeout as error:
            raise VerificationError(
                f"{self.name} did not emit readiness: " + " | ".join(self.tail)
            ) from error

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.forced_shutdown = True
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
        self.reader.join(timeout=5)
        if self.process.stdout is not None:
            self.process.stdout.close()

    def receipt(self) -> dict[str, Any]:
        return {
            "name": self.name, "pid": self.process.pid,
            "exit_code": self.process.returncode, "stdout_lines": self.lines,
            "forced_shutdown": self.forced_shutdown,
            "last_startup_phase": self.last_startup_phase,
        }


def run_cli(args: list[str], cwd: Path, env: dict[str, str]) -> str:
    result = subprocess.run(
        args, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace", timeout=30,
    )
    require(result.returncode == 0, "CLI command failed: " + " | ".join(
        redacted(line) for line in result.stdout.splitlines()[-8:]
    ))
    return result.stdout


def unpack(archive: Path, destination: Path) -> Path:
    require(digest(archive) == ARCHIVE_SHA256, "Official runtime archive SHA256 mismatch")
    with tarfile.open(archive, "r:gz") as bundle:
        members = bundle.getmembers()
        require(sum(member.size for member in members) < 2 * 1024**3, "Archive too large")
        seen: set[str] = set()
        for member in members:
            path = PurePosixPath(member.name)
            require(
                not path.is_absolute() and ".." not in path.parts
                and path.parts and path.parts[0] == BUNDLE_NAME
                and (member.isfile() or member.isdir()) and str(path) not in seen,
                "Unexpected archive entry",
            )
            seen.add(str(path))
            target = destination.joinpath(*path.parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            source = bundle.extractfile(member)
            require(source is not None, "Cannot read archive member")
            with source, target.open("xb") as output:
                shutil.copyfileobj(source, output)
            target.chmod(member.mode & 0o777)
    binary = destination / BUNDLE_NAME / "t3"
    require(binary.is_file() and os.access(binary, os.X_OK), "Runtime binary missing")
    return binary


def ready_json(line: str) -> dict[str, Any] | None:
    if not line.startswith("{"):
        return None
    value = json.loads(line)
    require(isinstance(value, dict), "Bridge readiness output is not an object")
    return value


def ready_upstream(line: str) -> bool | None:
    # This event follows listener activation and the command-ready gate. The
    # separate startup pairing/QR task must not be mistaken for server readiness.
    return True if "DEBUG" in line and line.endswith("startup phase: complete") else None


class Client:
    def __init__(self, address: tuple[str, int], origin: str, counters: dict[str, int]):
        self.address = address
        self.origin = origin
        self.authority = urlsplit(origin).netloc
        self.counters = counters

    def http(
        self, method: str, path: str, *, data: Any = None, cookie: str | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        headers = {"Host": self.authority, "Origin": self.origin, "Connection": "close"}
        if cookie is not None:
            headers["Cookie"] = cookie
        body = None
        if data is not None:
            body = json.dumps(data).encode("utf-8")
            headers["Content-Type"] = "application/json"
        connection = http.client.HTTPConnection(*self.address, timeout=SOCKET_TIMEOUT)
        self.counters["http_requests"] += 1
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            content = response.read(MAX_BODY + 1)
            require(len(content) <= MAX_BODY, "HTTP response exceeded verification limit")
            return response.status, dict((k.lower(), v) for k, v in response.getheaders()), content
        finally:
            connection.close()

    def websocket(self, *, cookie: str | None = None, origin: str | None = None) -> WebSocket:
        self.counters["websocket_upgrade_attempts"] += 1
        return WebSocket(self, cookie, origin)


class WebSocket:
    """Small RFC 6455 client for this verifier; no extension/compression negotiation."""

    def __init__(self, client: Client, cookie: str | None, origin: str | None):
        self.counters = client.counters
        self.socket = socket.create_connection(client.address, timeout=SOCKET_TIMEOUT)
        self.stream = self.socket.makefile("rb")
        self.status = 0
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        lines = [
            "GET /ws?orchestrationProtocol=2 HTTP/1.1", f"Host: {client.authority}",
            f"Origin: {origin or client.origin}", "Connection: Upgrade", "Upgrade: websocket",
            "Sec-WebSocket-Version: 13", f"Sec-WebSocket-Key: {key}",
        ]
        if cookie:
            lines.append("Cookie: " + cookie)
        try:
            self.socket.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("ascii"))
            status_line = self.stream.readline(8192).decode("ascii")
            require(status_line.startswith("HTTP/1.1 "), "Invalid WebSocket HTTP status")
            self.status = int(status_line.split(" ", 2)[1])
            headers: dict[str, str] = {}
            for _ in range(100):
                line = self.stream.readline(8192)
                require(bool(line), "EOF in WebSocket headers")
                if line == b"\r\n":
                    break
                name, value = line.decode("ascii").split(":", 1)
                headers[name.lower()] = value.strip()
            else:
                raise VerificationError("Too many WebSocket headers")
            if self.status == 101:
                expected = base64.b64encode(hashlib.sha1(
                    (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
                ).digest()).decode("ascii")
                require(headers.get("sec-websocket-accept") == expected, "Bad WebSocket accept hash")
                require(headers.get("upgrade", "").lower() == "websocket", "Missing upgrade header")
        except BaseException:
            self.close()
            raise

    def exact(self, size: int) -> bytes:
        value = self.stream.read(size)
        require(len(value) == size, "EOF in WebSocket frame")
        return value

    def send(self, opcode: int, payload: bytes) -> None:
        mask = os.urandom(4)
        size = len(payload)
        if size < 126:
            header = bytes([0x80 | opcode, 0x80 | size])
        elif size < 65536:
            header = bytes([0x80 | opcode, 0xFE]) + struct.pack("!H", size)
        else:
            header = bytes([0x80 | opcode, 0xFF]) + struct.pack("!Q", size)
        self.socket.sendall(header + mask + bytes(value ^ mask[i % 4] for i, value in enumerate(payload)))

    def send_json(self, value: Any) -> None:
        self.send(1, json.dumps(value, separators=(",", ":")).encode("utf-8"))
        self.counters["websocket_text_messages_sent"] += 1

    def receive_json(self) -> Any:
        assembled = bytearray()
        fragment = False
        for _ in range(100):
            first, second = self.exact(2)
            self.counters["websocket_frames_received"] += 1
            require(first & 0x70 == 0 and second & 0x80 == 0, "Unexpected WebSocket flags")
            opcode, final, size = first & 0x0F, bool(first & 0x80), second & 0x7F
            if size == 126:
                size = struct.unpack("!H", self.exact(2))[0]
            elif size == 127:
                size = struct.unpack("!Q", self.exact(8))[0]
            require(size + len(assembled) <= MAX_BODY, "WebSocket message exceeded verification limit")
            payload = self.exact(size)
            if opcode in (9, 10):
                require(final and size <= 125, "Invalid WebSocket control frame")
                if opcode == 9:
                    self.send(10, payload)
                continue
            require(opcode != 8, "WebSocket closed before the RPC response")
            require(opcode == (0 if fragment else 1), "Unexpected WebSocket opcode")
            assembled.extend(payload)
            if final:
                return json.loads(assembled)
            fragment = True
        raise VerificationError("Too many WebSocket fragments/control frames")

    def probe(self) -> None:
        self.send_json({"_tag": "Request", "id": "1", "tag": "server.probe", "payload": {}, "headers": []})
        for _ in range(20):
            message = self.receive_json()
            for item in message if isinstance(message, list) else [message]:
                require(isinstance(item, dict), "Malformed RPC response")
                if item.get("_tag") == "Ping":
                    self.send_json({"_tag": "Pong"})
                    continue
                if item.get("requestId") == "1":
                    require(item.get("_tag") == "Exit", "Unexpected server.probe response")
                    require(item.get("exit") == {"_tag": "Success", "value": {}}, "server.probe did not succeed")
                    self.counters["rpc_successes"] += 1
                    return
        raise VerificationError("No response to server.probe")

    def close(self) -> None:
        if self.status == 101:
            try:
                self.send(8, struct.pack("!H", 1000))
            except OSError:
                pass
        self.stream.close()
        self.socket.close()


def verify(args: argparse.Namespace, report: dict[str, Any], processes: list[Process]) -> None:
    require(sys.platform == "linux" and platform.machine() in ("x86_64", "amd64"), "Fixture requires Linux x86-64")
    lock = json.loads((WORKSPACE / "upstream.lock.json").read_text())
    require(lock["upstream"]["tag"] == TAG and lock["upstream"]["commit"] == COMMIT,
            "Upstream baseline changed; review/update this runtime fixture before running it")
    bridge = args.bridge.resolve(strict=True)
    report["bridge_sha256"] = digest(bridge)
    counters = report["counters"]

    def passed(name: str, **details: Any) -> None:
        report["checks"].append({"name": name, "status": "passed", **details})
        print(json.dumps({"check": name, "status": "passed"}), flush=True)

    with tempfile.TemporaryDirectory(prefix="t3-code-ls-upstream-", dir=args.scratch_dir) as directory:
        run_dir = Path(directory)
        runtime = unpack(args.archive, run_dir / "runtime")
        passed("official_archive_checksum", sha256=ARCHIVE_SHA256)
        project, user_home, state = (run_dir / name for name in ("project", "user-home", "t3-home"))
        for path in (project, user_home, state):
            path.mkdir()
        # A fresh process environment prevents provider credentials, custom origins,
        # cloud links, telemetry collectors, or dev auth from leaking into the check.
        env = {
            "PATH": "/usr/bin:/bin", "HOME": str(user_home),
            "XDG_CONFIG_HOME": str(user_home / ".config"),
            "XDG_DATA_HOME": str(user_home / ".local/share"),
            "XDG_CACHE_HOME": str(user_home / ".cache"), "TMPDIR": str(run_dir),
            "T3CODE_TELEMETRY_ENABLED": "false", "T3CODE_OTEL_SDK_DISABLED": "true",
            "NO_COLOR": "1", "TERM": "dumb", "LANG": "C.UTF-8",
        }
        require(run_cli([str(runtime), "--version"], project, env).strip() == "t3 " + TAG,
                "Wrong upstream binary version")
        # The upstream CLI rejects --port 0. If this port is taken before spawn,
        # startup must fail rather than probing/reusing someone else's server.
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        with ExitStack() as stack:
            def spawn(name: str, command: list[str], ready: Callable[[str], Any | None]) -> Process:
                process = Process(name, command, project, env, ready)
                processes.append(process)
                stack.callback(process.close)
                return process

            upstream = spawn("upstream", [
                str(runtime), "serve", "--host", "127.0.0.1", "--port", str(port),
                "--base-dir", str(state), "--no-browser", "--log-level", "debug",
            ], ready_upstream)
            upstream.await_ready()
            origin = f"http://127.0.0.1:{port}"
            passed("isolated_upstream_ready", loopback_only=True, fresh_state=True)
            direct = Client(("127.0.0.1", port), origin, counters)
            status, _, descriptor_body = direct.http("GET", "/.well-known/t3/environment")
            descriptor = json.loads(descriptor_body)
            require(status == 200 and descriptor.get("serverVersion") == VERSION
                    and descriptor.get("orchestrationProtocolVersion") == 2, "Wrong live upstream descriptor")
            passed("published_nightly_descriptor", server_version=VERSION, orchestration_protocol=2)

            client_key, host_key = run_dir / "client.key", run_dir / "host.key"
            client_id = run_cli([str(bridge), "identity", "--identity", str(client_key)], project, env).strip()
            host_id = run_cli([str(bridge), "identity", "--identity", str(host_key)], project, env).strip()
            require(re.fullmatch("[a-f0-9]{64}", client_id) is not None
                    and re.fullmatch("[a-f0-9]{64}", host_id) is not None
                    and client_id != host_id, "Invalid or identical bridge identities")
            host = spawn("iroh-host", [
                str(bridge), "serve", "--identity", str(host_key), "--target", f"127.0.0.1:{port}",
                "--allow", client_id, "--loopback",
            ], ready_json)
            host_ready = host.await_ready()
            endpoint = host_ready["endpoint"]
            require(endpoint["id"] == host_id, "Host identity changed")
            addresses = [item["Ip"] for item in endpoint["addrs"] if "Ip" in item and ":" in item["Ip"] and not item["Ip"].startswith("[")]
            require(bool(addresses) and all(address.startswith("127.0.0.1:") for address in addresses)
                    and all("Relay" not in item for item in endpoint["addrs"]), "Loopback Iroh address unavailable")
            forward = spawn("iroh-forward", [
                str(bridge), "forward", "--identity", str(client_key), "--remote", host_id,
                "--address", addresses[0], "--listen", "127.0.0.1:0", "--loopback",
            ], ready_json)
            forward_ready = forward.await_ready()
            require(forward_ready["endpoint_id"] == client_id, "Forwarder identity changed")
            forward_host, forward_port = forward_ready["listen"].rsplit(":", 1)
            require(forward_host == "127.0.0.1", "Forwarder did not bind loopback")
            passed("two_distinct_iroh_identities", routing="direct", public_discovery=False)
            forwarded_origin = f"http://{forward_host}:{int(forward_port)}"
            require(forwarded_origin != origin, "Upstream and forwarded origins must use different ports")
            for origin_mode, request_origin in (("upstream", origin), ("forwarded", forwarded_origin)):
                # A browser uses its forwarder's authority in Host and Origin.
                # Test both authorities without X-Forwarded-* or origin overrides.
                client = Client((forward_host, int(forward_port)), request_origin, counters)

                def checked(name: str, **details: Any) -> None:
                    passed(f"{origin_mode}_{name}", **details)

                direct_status, _, direct_html = direct.http("GET", "/")
                status, headers, html = client.http("GET", "/")
                require(direct_status == status == 200 and html == direct_html
                        and b"<html" in html.lower() and "text/html" in headers.get("content-type", ""),
                        "App HTML changed or failed through Iroh")
                checked("upstream_app_html", http_status=status, bytes=len(html), sha256=hashlib.sha256(html).hexdigest())

                status, _, body = client.http("GET", "/api/auth/session")
                session = json.loads(body)
                require(status == 200 and session.get("authenticated") is False, "Iroh peer admission authenticated the T3 session")
                require(session.get("auth", {}).get("policy") == "loopback-browser", "Upstream auth policy is not enabled")
                checked("anonymous_session", http_status=status, authenticated=False, policy="loopback-browser")
                status, _, body = client.http("GET", "/api/projects")
                require(status == 401 and json.loads(body).get("code") == "auth_invalid", "Protected HTTP endpoint allowed an anonymous peer")
                checked("anonymous_protected_http_denied", http_status=status)
                websocket = client.websocket()
                try:
                    require(websocket.status == 401, "Unauthenticated WebSocket was not rejected")
                    checked("anonymous_websocket_denied", http_status=websocket.status)
                finally:
                    websocket.close()

                status, _, body = client.http("POST", "/api/auth/browser-session", data={"credential": "invalid-runtime-verification-token"})
                require(status == 401 and json.loads(body).get("code") == "auth_invalid", "Invalid pairing token was accepted")
                checked("invalid_pairing_denied", http_status=status)
                # The local auth CLI is also used for headless deployment. Unlike
                # `t3 pair`, it does not enumerate network interfaces or render a QR.
                pairing = json.loads(run_cli([
                    str(runtime), "auth", "pairing", "create", "--base-dir", str(state),
                    "--scope", READ_SCOPE, "--ttl", "5 minutes", "--json",
                    "--label", "t3-code-ls runtime verifier",
                ], project, env))
                require(pairing.get("scopes") == [READ_SCOPE]
                        and isinstance(pairing.get("credential"), str), "Auth CLI did not issue a limited token")
                token = pairing["credential"]
                status, headers, body = client.http("POST", "/api/auth/browser-session", data={"credential": token})
                paired = json.loads(body)
                require(status == 200 and paired.get("authenticated") is True
                        and paired.get("permissions") == [READ_SCOPE], "Pairing did not preserve the limited grant")
                cookies = SimpleCookie(headers.get("set-cookie", ""))
                require(len(cookies) == 1, "Expected one browser session cookie")
                morsel = next(iter(cookies.values()))
                require(bool(morsel["httponly"]) and morsel["samesite"].lower() == "lax" and morsel["path"] == "/", "Browser cookie security attributes changed")
                cookie = morsel.key + "=" + morsel.value
                checked("limited_pairing_and_cookie", http_status=status, permissions=[READ_SCOPE], http_only=True, same_site="lax")
                status, _, body = client.http("GET", "/api/auth/session", cookie=cookie)
                session = json.loads(body)
                require(status == 200 and session.get("authenticated") is True
                        and session.get("permissions") == [READ_SCOPE]
                        and session.get("sessionMethod") == "browser-session-cookie", "Cookie session failed through Iroh")
                checked("authenticated_cookie_session", http_status=status, permissions=[READ_SCOPE])
                status, _, body = client.http("GET", "/api/projects", cookie=cookie)
                projects = json.loads(body)
                require(status == 200 and projects.get("projects") == [], "Fresh-state protected project read failed")
                checked("authenticated_protected_http", http_status=status, project_count=0)
                status, _, body = client.http("POST", "/api/auth/pairing-token", cookie=cookie, data={"scopes": [READ_SCOPE]})
                denied = json.loads(body)
                require(status == 403 and denied.get("code") == "insufficient_scope"
                        and denied.get("requiredPermission") == "access:write", "Read-only session did not preserve admin permission denial")
                checked("administrative_permission_denied", http_status=status, required_permission="access:write")
                status, _, body = client.http("POST", "/api/auth/browser-session", data={"credential": token})
                require(status == 401 and json.loads(body).get("code") == "auth_invalid", "One-time pairing token was reusable")
                checked("pairing_replay_denied", http_status=status)

                websocket = client.websocket(cookie=cookie, origin="http://invalid-origin.example")
                try:
                    require(websocket.status == 401, "Wrong-origin cookie WebSocket was not rejected")
                    checked("wrong_origin_websocket_denied", http_status=websocket.status)
                finally:
                    websocket.close()
                websocket = client.websocket(cookie=cookie)
                try:
                    require(websocket.status == 101, "Authenticated WebSocket upgrade failed")
                    websocket.probe()
                    checked("authenticated_websocket_rpc", http_status=101, method="server.probe", result="Success")
                finally:
                    websocket.close()
            require(all(process.process.poll() is None for process in processes), "Runtime process exited during verification")
    require(not state.exists() and not client_key.exists() and not host_key.exists(),
            "Temporary application state or identities survived cleanup")
    report["temporary_state_removed"] = True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--archive", required=True, type=Path, help="Official checksum-pinned linux-x64 release archive")
    parser.add_argument("--bridge", type=Path, default=WORKSPACE / "target/debug/t3-code-ls")
    parser.add_argument("--output", required=True, type=Path, help="JSON evidence path, outside the repository")
    parser.add_argument("--scratch-dir", type=Path, help="Parent directory for temporary runtime/state; removed after the run")
    args = parser.parse_args()
    if args.output.resolve().is_relative_to(WORKSPACE.parents[1]):
        parser.error("Evidence output must be outside the repository")
    report: dict[str, Any] = {
        "schema_version": 1, "status": "failed", "upstream": {
            "tag": TAG, "commit": COMMIT, "release_url": RELEASE_URL,
            "archive_sha256": ARCHIVE_SHA256,
        }, "checks": [], "counters": {
            "http_requests": 0, "websocket_upgrade_attempts": 0,
            "websocket_text_messages_sent": 0, "websocket_frames_received": 0, "rpc_successes": 0,
        }, "limits": [
            "Linux x86-64 with direct local Iroh routing only; no relay/NAT/mobile lifecycle coverage.",
            "No browser UI, provider turn, signed app, or full application parity verification.",
            "Original upstream and browser-forwarded Host/Origin paths tested; authentication remains upstream code.",
            "Backend sockets originate from loopback; upstream IP-based managed-policy behavior is not equivalent to native remote connections.",
        ],
    }
    processes: list[Process] = []
    try:
        verify(args, report, processes)
        require(all(process.process.returncode in ((0, 130) if process.name == "upstream" else (0,))
                    and not process.forced_shutdown for process in processes),
                "One or more processes required forced shutdown or exited unsuccessfully")
        report["status"] = "passed"
    except (Exception, KeyboardInterrupt) as error:
        report["error"] = str(error) if isinstance(error, VerificationError) else redacted(f"{type(error).__name__}: {error}")
    finally:
        report["processes"] = [process.receipt() for process in processes]
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"status": report["status"], "checks_passed": len(report["checks"])}), flush=True)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
