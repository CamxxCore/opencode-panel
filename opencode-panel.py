"""
opencode-panel - live control panel for an OpenCode server: workers, providers and MCP.

Tabs: Workers, Config, MCP, Tools, Logs.
Features:
    - Workers: live cards (SSE), right-click context menu, modal with
      tool-call tail / text tail / written files / pending permissions.
    - Config: opencode URL, server status + stop/start/restart,
      masked env-var view, API-key check with provider surface.
    - MCP: list configured + runtime connectors, add/remove, test.
    - Tools: universal OpenCode HTTP invoker (method + path + body).
      Response rendered as a pretty tree with a raw-JSON toggle.
    - Logs: the dashboard's own rolling log buffer.
    - Viewer: /view?path=<file> renders markdown/code with safe-root allowlist.

Stdlib only. Python 3.8+.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import shlex
import subprocess
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

DEFAULT_PORT = 7777
DEFAULT_OPENCODE = "http://127.0.0.1:4096"
POLL_INTERVAL_SEC = 2.0
IDLE_CUTOFF_MS = 300_000  # hide completed sessions idle > 5min
DEAD_CUTOFF_FACTOR = 6  # retry/failed/error sessions stay visible this many idle cutoffs
STALLED_MS = 60_000
OPEN_TURN_GRACE_MS = 15_000
WRITE_TOOLS = {"write", "edit", "multiedit"}

CONFIG = {
    "opencode_url": DEFAULT_OPENCODE,
    "filter_prefix": "",
    "cwd_filter": "",
    "idle_cutoff_ms": IDLE_CUTOFF_MS,
}

_LOCAL_HOSTS = {"127.0.0.1", "localhost", "[::1]"}

# Rolling log buffer surfaced on the Logs tab.
_LOG_BUF: deque = deque(maxlen=500)


def _log(line: str) -> None:
    _LOG_BUF.append(f"{time.strftime('%H:%M:%S')} {line}")


# ---------------------------------------------------------------------------
# OpenCode HTTP helpers
# ---------------------------------------------------------------------------

def _oc_status(method: str, path: str, body: Any = None, timeout: float = 3.0,
               _retry: int = 1, root: bool = False) -> tuple[int | None, Any]:
    """Return (http_status, payload); http_status is None when no response arrived.

    root=True strips any /api suffix so the call hits the server origin,
    for endpoints like prompt_async that only exist at root level.
    """
    base = CONFIG["opencode_url"].rstrip("/")
    if root and base.endswith("/api"):
        base = base[:-4]
    url = base + path
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            status = resp.status
    except urllib.error.HTTPError as exc:
        try:
            payload = json.loads(exc.read())
        except Exception:
            payload = {"error": str(exc)}
        return exc.code, {"__http_error__": exc.code, "__payload__": payload}
    except OSError as exc:
        # Only GET retries: a write whose response was lost may already have been applied.
        if method == "GET" and _retry > 0:
            time.sleep(0.15)
            return _oc_status(method, path, body, timeout, _retry - 1)
        _log(f"_oc {method} {path} connection-error: {exc}")
        return None, {"__error__": f"connection-error: {exc}"}
    if not raw:
        return status, None
    try:
        return status, json.loads(raw)
    except json.JSONDecodeError:
        return status, {"__error__": "non-json", "__preview__": raw[:200].decode(errors="replace"), "__status__": status}


def _oc(method: str, path: str, body: Any = None, timeout: float = 3.0, root: bool = False) -> Any:
    result = _oc_status(method, path, body, timeout, root=root)[1]
    if isinstance(result, dict) and "data" in result:
        return result["data"]
    return result


def _oc_error(res: Any) -> str | None:
    if isinstance(res, dict):
        if "__error__" in res:
            return str(res["__error__"])
        if "__http_error__" in res:
            return f"HTTP {res['__http_error__']}"
    return None


# ---------------------------------------------------------------------------
# Status derivation
# ---------------------------------------------------------------------------

def classify_session(sess: dict, last_msg: dict | None, has_pending_perm: bool,
                     now_ms: int, has_pending_question: bool = False,
                     live: dict | None = None) -> dict:
    """`live` is the session's /session/status entry, None when that endpoint is unavailable."""
    updated = int((sess.get("time") or {}).get("updated") or 0)
    age_ms = max(0, now_ms - updated) if updated else 0
    info = (last_msg or {}).get("info") or {}
    finish = info.get("finish")
    role = info.get("role")
    error = info.get("error") or {}
    live_type = (live or {}).get("type")
    if finish == "tool-calls":
        turn_stage = "tool-call"
    elif role == "user":
        turn_stage = "assistant pending"
    else:
        turn_stage = "streaming"
    status = "idle"
    stage = ""
    if has_pending_perm:
        status = "input_required"
        stage = "permission"
    elif has_pending_question:
        status = "input_required"
        stage = "question"
    elif live_type == "busy":
        status = "running"
        stage = "stalled" if age_ms >= STALLED_MS else turn_stage
    elif live_type == "retry":
        status = "retry"
        stage = f"attempt {live.get('attempt')}"
    elif last_msg is None:
        status = "accepted"
    elif error:
        if error.get("name") == "MessageAbortedError":
            status = "completed"; stage = "aborted"
        else:
            status = "error"; stage = str(error.get("name") or "error")
    elif role == "user" or finish in (None, "", "tool-calls"):
        # /session/status can lag a just-sent prompt by a moment, so an open turn gets a grace window.
        if age_ms < (OPEN_TURN_GRACE_MS if live is not None else STALLED_MS):
            status = "running"; stage = turn_stage
        else:
            status = "retry"; stage = "interrupted"
    elif finish == "stop":
        status = "completed" if ((info.get("time") or {}).get("completed")) else "idle"
        stage = "stop"
    else:
        status = str(finish); stage = str(finish)
    return {"status": status, "stage": stage, "last_update_ms": updated, "age_ms": age_ms,
            "live": live_type in ("busy", "retry")}


def is_active(derived: dict) -> bool:
    s = derived["status"]
    cutoff = CONFIG["idle_cutoff_ms"]
    if derived["live"] or s in ("running", "input_required"):
        return True
    if s in ("retry", "failed", "error"):
        return derived["age_ms"] < cutoff * DEAD_CUTOFF_FACTOR
    if s in ("accepted", "completed"):
        return derived["age_ms"] < cutoff
    return False


# ---------------------------------------------------------------------------
# Snapshot assembly
# ---------------------------------------------------------------------------

def extract_written_files(messages: list) -> list[str]:
    files = set()
    for m in messages or []:
        for p in m.get("parts", []) or []:
            if p.get("type") == "patch":
                for f in p.get("files", []) or []:
                    files.add(f.replace("/", os.sep))
            elif p.get("type") == "tool" and p.get("tool") in WRITE_TOOLS:
                inp = (p.get("state") or {}).get("input") or {}
                for key in ("filePath", "file_path", "filepath"):
                    v = inp.get(key)
                    if isinstance(v, str) and v:
                        files.add(v.replace("/", os.sep))
    return sorted(files)


def extract_tool_tail(messages: list, n: int = 10) -> list[dict]:
    out = []
    for m in messages or []:
        for p in m.get("parts", []) or []:
            if p.get("type") != "tool":
                continue
            state = p.get("state") or {}
            out.append({
                "tool": p.get("tool"),
                "status": state.get("status"),
                "input_preview": _short(state.get("input"), 160),
                "output_preview": _short(state.get("output"), 320),
                "err": state.get("error"),
                "started": (state.get("time") or {}).get("start"),
            })
    return out[-n:]


def extract_text_tail(messages: list, n_chars: int = 1200) -> str:
    for m in reversed(messages or []):
        if (m.get("info") or {}).get("role") != "assistant":
            continue
        for p in m.get("parts", []) or []:
            if p.get("type") == "text" and p.get("text"):
                return p["text"][-n_chars:]
    return ""


def _short(v: Any, n: int) -> str:
    if v is None:
        return ""
    s = v if isinstance(v, str) else json.dumps(v, default=str)
    return s if len(s) <= n else s[:n] + "..."


# Pending permission/question id -> "?directory=..." of the instance that owns it.
_REQUEST_DIRS: dict[str, str] = {}


def build_snapshot() -> dict:
    now_ms = int(time.time() * 1000)
    # /experimental/session spans every project; plain /session covers only the server's own directory.
    window_ms = CONFIG["idle_cutoff_ms"] * DEAD_CUTOFF_FACTOR
    sessions_list = _oc("GET", f"/experimental/session?start={now_ms - window_ms}&limit=500")
    server_ok = isinstance(sessions_list, list)
    if not server_ok:
        sessions_list = []
    active = _oc("GET", "/api/session/active")
    known = {s.get("id") for s in sessions_list}
    for sid in (active if isinstance(active, dict) else {}):
        if sid not in known:
            extra = _oc("GET", f"/session/{sid}")
            if isinstance(extra, dict) and extra.get("id"):
                sessions_list.append(extra)
    mcp = _oc("GET", "/mcp") or {}
    config = _oc("GET", "/config") or {}

    # Status, permissions and questions are scoped to the instance of the queried directory.
    live_by_sess: dict | None = {}
    permissions: list = []
    questions: list = []
    for directory in {s.get("directory") or "" for s in sessions_list}:
        q = "?directory=" + urllib.parse.quote(directory, safe="")
        live = _oc("GET", "/session/status" + q)
        if live_by_sess is not None:
            if isinstance(live, dict) and not _oc_error(live):
                live_by_sess.update(live)
            else:
                live_by_sess = None
        for target, path in ((permissions, "/permission"), (questions, "/question")):
            got = _oc("GET", path + q)
            if isinstance(got, list):
                target.extend(got)
                for item in got:
                    rid = item.get("id") or item.get("requestID")
                    if rid:
                        _REQUEST_DIRS[rid] = q

    perms_by_sess: dict[str, list] = {}
    for p in permissions:
        perms_by_sess.setdefault(p.get("sessionID") or "", []).append(p)
    qs_by_sess: dict[str, list] = {}
    for q in questions:
        qs_by_sess.setdefault(q.get("sessionID") or "", []).append(q)

    out_sessions = []
    hidden_by_filter = 0
    hidden_by_cwd = 0
    hidden_as_idle = 0
    for s in sessions_list:
        title = str(s.get("title") or "")
        if CONFIG["filter_prefix"] and not title.startswith(CONFIG["filter_prefix"]):
            hidden_by_filter += 1
            continue
        if CONFIG["cwd_filter"] and CONFIG["cwd_filter"] not in (s.get("directory") or ""):
            hidden_by_cwd += 1
            continue
        sid = s["id"]
        msgs = _oc("GET", f"/session/{sid}/message?limit=20", timeout=1.5)
        if not isinstance(msgs, list):
            msgs = []
        last_asst = None
        for m in reversed(msgs):
            if (m.get("info") or {}).get("role") == "assistant":
                last_asst = m; break
        if last_asst is None and msgs:
            last_asst = msgs[-1]
        live = None if live_by_sess is None else live_by_sess.get(sid, {"type": "idle"})
        derived = classify_session(s, last_asst, bool(perms_by_sess.get(sid)), now_ms,
                                   bool(qs_by_sess.get(sid)), live)
        if not is_active(derived):
            hidden_as_idle += 1
            continue
        tokens = s.get("tokens") or {}
        cache = tokens.get("cache") or {}
        model = s.get("model") or {}
        out_sessions.append({
            "id": sid,
            "title": title,
            "slug": s.get("slug"),
            "directory": s.get("directory"),
            "status": derived["status"],
            "stage": derived["stage"],
            "age_ms": derived["age_ms"],
            "updated": derived["last_update_ms"],
            "model_provider": model.get("providerID"),
            "model_id": model.get("id"),
            "tokens": {
                "input": tokens.get("input") or 0,
                "output": tokens.get("output") or 0,
                "reasoning": tokens.get("reasoning") or 0,
                "cache_read": cache.get("read") or 0,
            },
            "pending_permissions": perms_by_sess.get(sid, []),
            "pending_questions": qs_by_sess.get(sid, []),
            "written_files": extract_written_files(msgs),
            "tool_tail": extract_tool_tail(msgs),
            "text_tail": extract_text_tail(msgs),
        })

    out_sessions.sort(key=lambda s: (
        {"input_required": 0, "retry": 0, "running": 1, "accepted": 2,
         "failed": 3, "error": 3, "completed": 4, "idle": 5}.get(s["status"], 6),
        -s["updated"],
    ))

    orphan_perms = [p for p in permissions if (p.get("sessionID") or "") not in {s["id"] for s in out_sessions}]
    orphan_qs = [q for q in questions if (q.get("sessionID") or "") not in {s["id"] for s in out_sessions}]

    return {
        "now_ms": now_ms,
        "config": {k: CONFIG[k] for k in ("opencode_url", "filter_prefix", "cwd_filter", "idle_cutoff_ms")},
        "server": {"ok": server_ok},
        "mcp_runtime": mcp if isinstance(mcp, dict) else {"__error__": str(mcp)},
        "mcp_configured": (config.get("mcp") or {}) if isinstance(config, dict) else {},
        "sessions": out_sessions,
        "orphan_permissions": orphan_perms,
        "orphan_questions": orphan_qs,
        "hidden": {
            "by_filter": hidden_by_filter,
            "by_cwd": hidden_by_cwd,
            "as_idle": hidden_as_idle,
            "total_raw": len(sessions_list),
        },
    }


# ---------------------------------------------------------------------------
# SSE
# ---------------------------------------------------------------------------

_subscribers: list[queue.Queue] = []
_subs_lock = threading.Lock()


def _broadcast(event: str, data: Any) -> None:
    payload = f"event: {event}\ndata: {json.dumps(data)}\n\n"
    with _subs_lock:
        for q in list(_subscribers):
            try: q.put_nowait(payload)
            except queue.Full: pass


def poller_loop() -> None:
    last_hash = None
    while True:
        try:
            snap = build_snapshot()
            key = tuple((s["id"], s["status"], s["stage"], s["updated"]) for s in snap["sessions"])
            if key != last_hash or snap["orphan_permissions"] or snap["orphan_questions"]:
                _broadcast("snapshot", snap); last_hash = key
            else:
                _broadcast("tick", {"now_ms": snap["now_ms"]})
        except Exception:
            _log("poller error: " + traceback.format_exc())
            _broadcast("err", {"trace": traceback.format_exc()})
        time.sleep(POLL_INTERVAL_SEC)


# ---------------------------------------------------------------------------
# Server control (OpenCode daemon)
# ---------------------------------------------------------------------------

def _is_opencode_alive() -> bool:
    try:
        with urllib.request.urlopen(CONFIG["opencode_url"] + "/config", timeout=1.5) as r:
            return r.status == 200
    except Exception:
        return False


_proc_cache = {"ts": 0.0, "value": None}
_OPENCODE_SERVE_SCAN = (
    "Get-WmiObject Win32_Process -Filter \"Name='node.exe' or Name='opencode.exe'\" | "
    "Where-Object { $_.CommandLine -match 'opencode\\S*\\s+serve(\\s|$)' } | "
)


def _opencode_processes(force: bool = False) -> list[int]:
    """Return pids of node/opencode.exe processes running `opencode serve`.

    Cached 10 s because the PowerShell + WMI spawn costs ~1 s per call
    and the admin endpoint hits this on every browser tick."""
    now = time.time()
    if not force and _proc_cache["value"] is not None and now - _proc_cache["ts"] < 10.0:
        return _proc_cache["value"]
    try:
        ps = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             _OPENCODE_SERVE_SCAN + "Select-Object -ExpandProperty ProcessId"],
            capture_output=True, text=True, timeout=5,
        )
        pids = [int(x) for x in ps.stdout.split() if x.strip().isdigit()]
    except Exception as exc:
        _log(f"process scan failed: {exc}")
        pids = []
    _proc_cache["ts"] = now
    _proc_cache["value"] = pids
    return pids


def opencode_stop() -> dict:
    pids = _opencode_processes(force=True)
    if not pids:
        return {"ok": True, "note": "no opencode processes found"}
    killed = []
    for pid in pids:
        try:
            subprocess.run(["powershell", "-NoProfile", "-Command", f"Stop-Process -Id {pid} -Force"],
                           capture_output=True, timeout=5)
            killed.append(pid)
        except Exception as exc:
            _log(f"stop pid {pid}: {exc}")
    time.sleep(1)
    return {"ok": True, "killed": killed}


def opencode_start() -> dict:
    if _is_opencode_alive():
        return {"ok": True, "note": "already running"}
    oc_url = urllib.parse.urlsplit(CONFIG["opencode_url"])
    try:
        subprocess.Popen(
            ["cmd.exe", "/c", "start", "/b", "opencode.cmd", "serve",
             "--hostname", oc_url.hostname or "127.0.0.1", "--port", str(oc_url.port or 4096)],
            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True,
        )
    except Exception as exc:
        _log(f"start failed: {exc}")
        return {"ok": False, "error": str(exc)}
    # Wait for health
    for _ in range(30):
        time.sleep(0.5)
        if _is_opencode_alive():
            return {"ok": True, "note": "started"}
    return {"ok": False, "error": "server did not come up within 15s"}


def opencode_restart() -> dict:
    stop = opencode_stop()
    start = opencode_start()
    return {"stop": stop, "start": start, "alive": _is_opencode_alive()}


TRACKED_ENV_KEYS = [
    "OPENCODE_API_KEY", "OPENCODE_URL", "OPENCODE_BASE_URL",
    "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GOOGLE_API_KEY",
    "DEEPSEEK_API_KEY", "MISTRAL_API_KEY", "GROQ_API_KEY",
    "HF_TOKEN", "GITHUB_TOKEN", "OPENROUTER_API_KEY",
    "CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID", "CLOUDFLARE_GATEWAY_ID",
]


def _mask(v: str) -> str:
    if not v:
        return ""
    return f"{v[:4]}...{v[-4:]}" if len(v) > 10 else "set"


def opencode_env_mask() -> dict:
    """Mask this dashboard process's env; the running daemon's env is not readable from here."""
    return {k: (_mask(os.environ[k]) if k in os.environ else None) for k in TRACKED_ENV_KEYS}


def _claude_mcp_envs(d: dict):
    """Yield (location, server_name, env) for each MCP server in a ~/.claude.json document."""
    for name, cfg in (d.get("mcpServers") or {}).items():
        yield f"mcpServers.{name}", name, (cfg or {}).get("env") or {}
    for proj, pcfg in (d.get("projects") or {}).items():
        for name, cfg in ((pcfg or {}).get("mcpServers") or {}).items():
            where = f"projects[{os.path.basename(proj.rstrip('/'))}].mcpServers.{name}"
            yield where, name, (cfg or {}).get("env") or {}


def discover_api_keys() -> dict:
    """Scan Claude Code config + OpenCode auth files for API keys.

    Returns {source: {key_name: masked_value}} so the Config tab can show the
    user which keys exist in which source, not just the dashboard process env.
    """
    found: dict[str, dict[str, str]] = {}
    # Claude Code user config
    cj = os.path.expanduser("~/.claude.json")
    if os.path.exists(cj):
        try:
            with open(cj, "r", encoding="utf-8") as f:
                d = json.load(f)
            src: dict[str, str] = {}
            for where, _, env in _claude_mcp_envs(d):
                for k, v in env.items():
                    if k in TRACKED_ENV_KEYS and isinstance(v, str) and v:
                        src[f"{k} (via {where})"] = _mask(v)
            if src:
                found["~/.claude.json"] = src
        except Exception as exc:
            found["~/.claude.json"] = {"__error__": str(exc)}

    # OpenCode's own auth file (if present)
    data_home = os.environ.get("XDG_DATA_HOME") or os.path.expanduser("~/.local/share")
    for cand in (os.path.join(data_home, "opencode", "auth.json"),):
        if cand and os.path.exists(cand):
            try:
                with open(cand, "r", encoding="utf-8") as f:
                    d = json.load(f)
                src = {}
                for k, v in d.items():
                    if isinstance(v, str) and v:
                        src[k] = _mask(v)
                    elif isinstance(v, dict):
                        for kk, vv in v.items():
                            if isinstance(vv, str) and vv:
                                src[f"{k}.{kk}"] = _mask(vv)
                if src:
                    found[cand] = src
            except Exception as exc:
                found[cand] = {"__error__": str(exc)}

    return found


_uptime_cache = {"ts": 0.0, "value": None}


def opencode_server_uptime() -> dict:
    """Query the opencode process creation time via WMI; return uptime seconds.

    Cached 10 s, same reason as `_opencode_processes`."""
    now = time.time()
    if _uptime_cache["value"] is not None and now - _uptime_cache["ts"] < 10.0:
        # Keep uptime_sec rolling even on cache hits so the UI doesn't look frozen.
        snap = dict(_uptime_cache["value"])
        if snap.get("start_time"):
            try:
                import datetime
                dt = datetime.datetime.fromisoformat(snap["start_time"].rstrip("Z"))
                tnow = datetime.datetime.now(dt.tzinfo) if dt.tzinfo else datetime.datetime.now()
                snap["uptime_sec"] = int((tnow - dt).total_seconds())
            except Exception:
                pass
        return snap

    pids = _opencode_processes()
    if not pids:
        value = {"pids": [], "uptime_sec": None, "start_time": None}
        _uptime_cache["ts"] = now
        _uptime_cache["value"] = value
        return value
    try:
        ps = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             _OPENCODE_SERVE_SCAN +
             "Select-Object ProcessId,@{Name='StartTime';Expression={$_.ConvertToDateTime($_.CreationDate).ToString('o')}} | "
             "ConvertTo-Json -Compress"],
            capture_output=True, text=True, timeout=5,
        )
        data = json.loads(ps.stdout) if ps.stdout.strip() else []
        if isinstance(data, dict):
            data = [data]
        import datetime
        earliest = None
        for row in data:
            try:
                dt = datetime.datetime.fromisoformat(row["StartTime"].rstrip("Z"))
            except Exception:
                continue
            if earliest is None or dt < earliest:
                earliest = dt
        if earliest is None:
            value = {"pids": pids, "uptime_sec": None, "start_time": None}
        else:
            tnow = datetime.datetime.now(earliest.tzinfo) if earliest.tzinfo else datetime.datetime.now()
            uptime = (tnow - earliest).total_seconds()
            value = {"pids": pids, "uptime_sec": int(uptime), "start_time": earliest.isoformat()}
    except Exception as exc:
        _log(f"uptime query failed: {exc}")
        value = {"pids": pids, "uptime_sec": None, "start_time": None}
    _uptime_cache["ts"] = now
    _uptime_cache["value"] = value
    return value


_api_key_health_cache = {"ts": 0.0, "value": None}
_models_cache: dict = {"ts": 0.0, "value": None}


def api_key_health() -> dict:
    """Summarise /provider for the status strip. Cached 30 s because the
    full provider catalog is multi-megabyte and the client calls admin
    on every tick."""
    now = time.time()
    cached = _api_key_health_cache
    if cached["value"] is not None and now - cached["ts"] < 30.0:
        return cached["value"]
    prov = _oc("GET", "/provider", timeout=10.0)
    if not isinstance(prov, dict):
        result = {"ok": False, "detail": "provider endpoint unreachable"}
    else:
        all_providers = prov.get("all") or []
        oc_prov = next((p for p in all_providers if p.get("id") == "opencode"), None)
        if not oc_prov:
            result = {"ok": False, "detail": "opencode provider not listed",
                      "providers_total": len(all_providers)}
        else:
            result = {
                "ok": True,
                "connected": "opencode" in (prov.get("connected") or []),
                "opencode_models": list((oc_prov.get("models") or {}).keys()),
                "providers_total": len(all_providers),
            }
    _api_key_health_cache["ts"] = now
    _api_key_health_cache["value"] = result
    return result


def opencode_env_set(key: str, value: str | None) -> dict:
    """Set an env var in this Python process's environment. Does NOT persist
    beyond this process. For durable change the user must set it system-wide
    and restart opencode. We just set it + restart opencode so the new
    process (which we start as a child) inherits it."""
    if value in (None, ""):
        os.environ.pop(key, None)
    else:
        os.environ[key] = value
    return {"ok": True, "note": "env set in dashboard process; restart opencode from this panel to apply it"}


# ---------------------------------------------------------------------------
# Theme
# ---------------------------------------------------------------------------

_DARK_TOKENS = """
  color-scheme: dark;
  --bg: #141312; --surface: #1b1a19; --surface-2: #232220; --inset: #0f0e0d;
  --border: #2c2a28; --border-strong: #3b3835;
  --text: #ebe7e2; --muted: #a29c95; --faint: #6f6a64;
  --accent: #e49a72; --accent-hover: #efae8c; --accent-fg: #1c1310; --accent-tint: rgba(228,154,114,.14);
  --ok: #8cc79a; --ok-tint: rgba(140,199,154,.14);
  --warn: #e3b45e; --warn-tint: rgba(227,180,94,.15);
  --danger: #ec7f74; --danger-tint: rgba(236,127,116,.14);
  --info: #90b8ec; --info-tint: rgba(144,184,236,.14);
  --violet: #b7a0f2; --violet-tint: rgba(183,160,242,.15);
  --overlay: rgba(8,7,6,.62);
  --shadow: 0 1px 2px rgba(0,0,0,.35), 0 12px 32px rgba(0,0,0,.35);
"""

_LIGHT_TOKENS = """
  color-scheme: light;
  --bg: #f7f6f4; --surface: #ffffff; --surface-2: #f1efec; --inset: #f4f2ef;
  --border: #e5e1dc; --border-strong: #d3cec8;
  --text: #24211f; --muted: #6c6762; --faint: #a19b95;
  --accent: #c2622f; --accent-hover: #ad5426; --accent-fg: #ffffff; --accent-tint: rgba(194,98,47,.10);
  --ok: #2d8546; --ok-tint: rgba(45,133,70,.10);
  --warn: #9a6a0c; --warn-tint: rgba(154,106,12,.11);
  --danger: #c0443a; --danger-tint: rgba(192,68,58,.10);
  --info: #2f63ae; --info-tint: rgba(47,99,174,.10);
  --violet: #6b4fc4; --violet-tint: rgba(107,79,196,.10);
  --overlay: rgba(36,33,31,.32);
  --shadow: 0 1px 2px rgba(36,33,31,.06), 0 12px 32px rgba(36,33,31,.12);
"""

THEME_CSS = (
    ":root{" + _DARK_TOKENS + """
  --radius: 10px; --radius-sm: 7px;
  --font: -apple-system, BlinkMacSystemFont, "Segoe UI Variable Text", "Segoe UI", Inter, Roboto, system-ui, sans-serif;
  --mono: ui-monospace, "Cascadia Code", "SF Mono", "JetBrains Mono", Menlo, Consolas, monospace;
}
@media (prefers-color-scheme: light){ :root:not([data-theme="dark"]){""" + _LIGHT_TOKENS + """} }
:root[data-theme="light"]{""" + _LIGHT_TOKENS + """}
*{box-sizing:border-box;}
html,body{margin:0;background:var(--bg);color:var(--text);font-family:var(--font);font-size:13px;line-height:1.5;
  -webkit-font-smoothing:antialiased;}
a{color:var(--accent);text-decoration:none;} a:hover{text-decoration:underline;}
code{font-family:var(--mono);font-size:12px;background:var(--surface-2);border:1px solid var(--border);
  border-radius:5px;padding:0 5px;color:var(--text);}
::selection{background:var(--accent-tint);}
*{scrollbar-width:thin;scrollbar-color:var(--border-strong) transparent;}
::-webkit-scrollbar{width:10px;height:10px;}
::-webkit-scrollbar-thumb{background:var(--border-strong);border-radius:10px;border:3px solid var(--bg);}
::-webkit-scrollbar-track{background:transparent;}
"""
)


# ---------------------------------------------------------------------------
# File viewer
# ---------------------------------------------------------------------------

_VIEWER_ROOTS: list[str] = []  # filled in main()


def _is_safe_path(path: str) -> bool:
    try:
        resolved = os.path.realpath(path)
    except Exception:
        return False
    rl = resolved.lower()
    for root in _VIEWER_ROOTS:
        base = os.path.realpath(root).lower()
        if rl == base or rl.startswith(base + os.sep):
            return True
    return False


_HIGHLIGHT_EXTS = {
    ".md": "markdown", ".py": "python", ".js": "javascript", ".ts": "typescript",
    ".c": "c", ".cpp": "cpp", ".h": "c", ".hpp": "cpp", ".cs": "csharp",
    ".json": "json", ".yaml": "yaml", ".yml": "yaml", ".xml": "xml",
    ".sh": "bash", ".ps1": "powershell", ".bat": "batch", ".rs": "rust",
    ".go": "go", ".rb": "ruby", ".txt": "plaintext",
}


def _render_markdown(text: str) -> str:
    import html as _html, re
    lines = text.splitlines()
    out = []
    i = 0
    in_code = False
    code_buf = []
    code_lang = ""
    in_list = False
    list_type = None

    def close_list():
        nonlocal in_list, list_type
        if in_list:
            out.append(f"</{list_type}>")
            in_list = False
            list_type = None

    while i < len(lines):
        ln = lines[i]
        if in_code:
            if ln.strip().startswith("```"):
                esc = _html.escape("\n".join(code_buf))
                out.append(f'<pre class="code" data-lang="{_html.escape(code_lang)}"><code>{esc}</code></pre>')
                in_code = False; code_buf = []; code_lang = ""
            else:
                code_buf.append(ln)
            i += 1; continue
        m = re.match(r"^```(\w*)\s*$", ln.strip())
        if m:
            close_list(); in_code = True; code_lang = m.group(1); i += 1; continue
        m = re.match(r"^(#{1,6})\s+(.*)$", ln)
        if m:
            close_list(); lvl = len(m.group(1))
            out.append(f"<h{lvl}>{_inline_md(m.group(2))}</h{lvl}>"); i += 1; continue
        m = re.match(r"^\s*[-*+]\s+(.*)$", ln)
        if m:
            if not in_list or list_type != "ul":
                close_list(); in_list = True; list_type = "ul"; out.append("<ul>")
            out.append(f"<li>{_inline_md(m.group(1))}</li>"); i += 1; continue
        m = re.match(r"^\s*\d+\.\s+(.*)$", ln)
        if m:
            if not in_list or list_type != "ol":
                close_list(); in_list = True; list_type = "ol"; out.append("<ol>")
            out.append(f"<li>{_inline_md(m.group(1))}</li>"); i += 1; continue
        if ln.strip() == "":
            close_list(); out.append("")
        else:
            close_list(); out.append(f"<p>{_inline_md(ln)}</p>")
        i += 1
    if in_code:
        out.append(f"<pre class=\"code\"><code>{_html.escape(chr(10).join(code_buf))}</code></pre>")
    close_list()
    return "\n".join(out)


def _inline_md(s: str) -> str:
    import html as _html, re
    codes = []
    def _code(m):
        codes.append(m.group(1))
        return f"\x00CODE{len(codes)-1}\x00"
    s = re.sub(r"`([^`]+)`", _code, s)
    s = _html.escape(s)
    s = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", s)
    s = re.sub(r"\*([^*]+)\*", r"<em>\1</em>", s)
    def _link(m):
        scheme = urllib.parse.urlsplit(_html.unescape(m.group(2)).strip()).scheme.lower()
        return f'<a href="{m.group(2)}">{m.group(1)}</a>' if scheme in ("http", "https") else m.group(1)
    s = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", _link, s)
    for i, c in enumerate(codes):
        s = s.replace(f"\x00CODE{i}\x00", f"<code>{_html.escape(c)}</code>")
    return s


def render_file_view(target: str) -> str:
    import html as _html
    if not target:
        return "<p>missing path</p>"
    norm = os.path.normpath(target)
    if not os.path.isabs(norm) or not os.path.exists(norm) or os.path.isdir(norm):
        return f"<p>not a file: {_html.escape(target)}</p>"
    if not _is_safe_path(norm):
        return "<p>path blocked by viewer root allowlist</p>"
    try:
        size = os.path.getsize(norm)
        if size > 5 * 1024 * 1024:
            return f"<p>file too large to view ({size} bytes)</p>"
        with open(norm, "r", encoding="utf-8", errors="replace") as f:
            raw = f.read()
    except Exception as exc:
        return f"<p>read error: {_html.escape(str(exc))}</p>"
    ext = os.path.splitext(norm)[1].lower()
    lang = _HIGHLIGHT_EXTS.get(ext, "plaintext")
    body = _render_markdown(raw) if ext == ".md" else f'<pre class="code" data-lang="{_html.escape(lang)}"><code>{_html.escape(raw)}</code></pre>'
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>view: {_html.escape(os.path.basename(norm))}</title>
<style>
{THEME_CSS}
 .topbar{{display:flex;align-items:center;gap:12px;padding:10px 16px;background:color-mix(in srgb,var(--bg) 88%,transparent);
   backdrop-filter:blur(8px);border-bottom:1px solid var(--border);position:sticky;top:0;z-index:10;}}
 .topbar a{{color:var(--text);font-size:12px;white-space:nowrap;padding:5px 11px;border:1px solid var(--border);border-radius:999px;background:var(--surface);}}
 .topbar a:hover{{background:var(--surface-2);text-decoration:none;}}
 .topbar .path{{color:var(--muted);font-family:var(--mono);font-size:12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1 1 auto;}}
 .topbar .info{{color:var(--faint);font-size:11px;white-space:nowrap;}}
 .wrap{{padding:20px 24px 48px;max-width:900px;margin:0 auto;}}
 h1,h2,h3,h4,h5,h6{{color:var(--text);margin:28px 0 10px;line-height:1.3;font-weight:600;}}
 h1{{font-size:22px;}} h2{{font-size:17px;padding-bottom:6px;border-bottom:1px solid var(--border);}} h3{{font-size:14px;}}
 p,ul,ol{{color:var(--text);}} p{{margin:10px 0;}} li{{margin:3px 0;}}
 pre.code{{background:var(--inset);border:1px solid var(--border);border-radius:var(--radius);padding:12px 14px;overflow:auto;font-size:12px;line-height:1.55;}}
 pre.code code{{background:none;border:0;padding:0;}}
 strong{{color:var(--text);}} em{{color:var(--muted);}}
</style></head><body>
<div class="topbar"><a href="/">&larr; Dashboard</a>
<div class="path" title="{_html.escape(norm)}">{_html.escape(norm)}</div>
<span class="info">{size} bytes &middot; {lang}</span></div>
<div class="wrap">{body}</div></body></html>
"""


# ---------------------------------------------------------------------------
# Index HTML
# ---------------------------------------------------------------------------

INDEX_HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>OpenCode Panel</title>
<script>(function(){var t=new URLSearchParams(location.search).get('theme');if(t==='light'||t==='dark')document.documentElement.dataset.theme=t;})();</script>
<style>
""" + THEME_CSS + r"""
 button{font:inherit;font-size:12px;font-weight:500;color:var(--text);background:var(--surface);border:1px solid var(--border);
   padding:5px 11px;border-radius:var(--radius-sm);cursor:pointer;transition:background .12s,border-color .12s,color .12s;}
 button:hover{background:var(--surface-2);border-color:var(--border-strong);}
 button:focus-visible,input:focus-visible,textarea:focus-visible,select:focus-visible,.card:focus-visible{outline:2px solid var(--accent);outline-offset:1px;}
 button:disabled{opacity:.45;cursor:not-allowed;}
 button.primary{background:var(--accent);border-color:var(--accent);color:var(--accent-fg);}
 button.primary:hover{background:var(--accent-hover);border-color:var(--accent-hover);}
 button.good{color:var(--ok);} button.good:hover:not(:disabled){background:var(--ok-tint);border-color:transparent;}
 button.danger{color:var(--danger);} button.danger:hover{background:var(--danger-tint);border-color:transparent;}
 input,textarea,select{font:inherit;font-size:12px;color:var(--text);background:var(--inset);border:1px solid var(--border);
   padding:7px 10px;border-radius:var(--radius-sm);}
 input:focus,textarea:focus,select:focus{border-color:var(--accent);outline:none;box-shadow:0 0 0 3px var(--accent-tint);}
 input::placeholder,textarea::placeholder{color:var(--faint);}
 textarea{width:100%;resize:vertical;font-family:var(--mono);}
 input{width:100%;}
 label{color:var(--muted);font-size:12px;}
 .wrap{padding:14px 16px 32px;max-width:1180px;margin:0 auto;}

 /* Header */
 .top{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:12px;flex-wrap:wrap;}
 .top h1{margin:0;font-size:15px;font-weight:600;letter-spacing:-.01em;display:flex;align-items:center;gap:9px;}
 .top h1 .mark{width:22px;height:22px;border-radius:7px;display:grid;place-items:center;background:var(--accent-tint);color:var(--accent);}
 .meta{color:var(--muted);font-size:12px;display:flex;align-items:center;gap:10px;}
 .clock{color:var(--faint);font-variant-numeric:tabular-nums;}
 .fire-btn{background:var(--accent);border-color:var(--accent);color:var(--accent-fg);font-weight:600;}
 .fire-btn:hover{background:var(--accent-hover);border-color:var(--accent-hover);}

 /* Status strip */
 .statusstrip{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:12px;}
 .statusstrip .cell{display:inline-flex;align-items:baseline;gap:6px;min-width:0;max-width:100%;padding:4px 10px;
   background:var(--surface);border:1px solid var(--border);border-radius:999px;font-size:12px;}
 .statusstrip .cell .lbl{color:var(--faint);font-size:11px;white-space:nowrap;}
 .statusstrip .cell .val{color:var(--text);font-variant-numeric:tabular-nums;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
 .statusstrip .cell.ok .val{color:var(--ok);} .statusstrip .cell.bad .val{color:var(--danger);} .statusstrip .cell.warn .val{color:var(--warn);}
 .statusstrip .cell.ok::before,.statusstrip .cell.bad::before,.statusstrip .cell.warn::before{content:"";width:7px;height:7px;border-radius:50%;
   align-self:center;background:currentColor;}
 .statusstrip .cell.ok{color:var(--ok);} .statusstrip .cell.bad{color:var(--danger);} .statusstrip .cell.warn{color:var(--warn);}

 /* Tabs */
 .tabs{display:inline-flex;gap:2px;padding:3px;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);
   margin-bottom:14px;max-width:100%;overflow-x:auto;}
 .tabs button{background:transparent;border:1px solid transparent;color:var(--muted);padding:5px 12px;border-radius:var(--radius-sm);white-space:nowrap;}
 .tabs button:hover{color:var(--text);background:var(--surface-2);}
 .tabs button.active{color:var(--text);background:var(--surface-2);border-color:var(--border-strong);}
 .tabs .spacer{flex:1 1 auto;}

 .ok{color:var(--ok);font-weight:500;} .bad{color:var(--danger);font-weight:500;}
 .alert{background:var(--warn-tint);border:1px solid transparent;color:var(--warn);padding:10px 14px;border-radius:var(--radius);margin-bottom:12px;}
 .alert ul{margin:6px 0 0;padding-left:18px;color:var(--text);font-size:12px;}
 .pane{display:none;} .pane.active{display:block;animation:fade .15s ease;}
 @keyframes fade{from{opacity:0;transform:translateY(2px)}to{opacity:1;transform:none}}
 #mcp-mini{margin-bottom:10px;color:var(--faint);font-size:12px;}

 /* Worker cards */
 #sessions{display:grid;gap:8px;}
 .card{position:relative;background:var(--surface);border:1px solid var(--border);padding:12px 14px 12px 16px;border-radius:var(--radius);
   cursor:pointer;transition:border-color .12s,background .12s;}
 .card:hover{border-color:var(--border-strong);}
 .card::before{content:"";position:absolute;left:0;top:12px;bottom:12px;width:3px;border-radius:0 3px 3px 0;background:var(--border-strong);}
 .card.running::before{background:var(--info);}
 .card.accepted::before{background:var(--violet);}
 .card.input_required::before,.card.retry::before{background:var(--warn);}
 .card.failed::before,.card.error::before,.card.stalled::before{background:var(--danger);}
 .card.completed,.card.idle{background:transparent;}
 .card.completed .title,.card.idle .title{color:var(--muted);}
 .card .row1{display:flex;justify-content:space-between;align-items:flex-start;gap:10px;}
 .card .title{font-weight:600;color:var(--text);min-width:0;overflow-wrap:anywhere;}
 .card .row1 > div:last-child{flex-shrink:0;}
 .status{display:inline-flex;align-items:center;gap:6px;padding:2px 9px 2px 8px;border-radius:999px;font-size:11px;font-weight:500;
   white-space:nowrap;color:var(--muted);background:var(--surface-2);}
 .status::before{content:"";width:6px;height:6px;border-radius:50%;background:currentColor;}
 .status.running{color:var(--info);background:var(--info-tint);}
 .status.running::before{animation:pulse 1.4s ease-in-out infinite;}
 .status.completed{color:var(--ok);background:var(--ok-tint);}
 .status.accepted{color:var(--violet);background:var(--violet-tint);}
 .status.input_required,.status.retry{color:var(--warn);background:var(--warn-tint);}
 .status.failed,.status.error{color:var(--danger);background:var(--danger-tint);}
 @keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
 .card .sid{color:var(--faint);font-size:11px;font-family:var(--mono);}
 .card .meta2,.card .tok{margin-top:6px;color:var(--muted);font-size:12px;display:flex;gap:4px 14px;flex-wrap:wrap;}
 .card .meta2 code{background:transparent;border:0;padding:0;color:var(--muted);font-size:11.5px;overflow-wrap:anywhere;}
 .card .meta2 span:first-child code{color:var(--text);}
 .card .tok{color:var(--faint);font-variant-numeric:tabular-nums;}
 .in,.out,.rsn{color:var(--text);}
 .explain{color:var(--muted);font-size:12px;margin-top:6px;padding:7px 10px;background:var(--surface-2);border-radius:var(--radius-sm);}
 .cardactions{display:flex;gap:6px;margin-top:10px;flex-wrap:wrap;}
 .cardactions button{padding:4px 10px;font-size:12px;}
 .empty{color:var(--faint);padding:40px 16px;text-align:center;border:1px dashed var(--border-strong);border-radius:var(--radius);}

 /* Context menu */
 .ctxmenu{position:fixed;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:4px;min-width:190px;
   z-index:200;box-shadow:var(--shadow);display:none;}
 .ctxmenu.open{display:block;}
 .ctxmenu button{display:block;width:100%;text-align:left;background:transparent;border:0;color:var(--text);padding:7px 10px;border-radius:var(--radius-sm);}
 .ctxmenu button:hover{background:var(--surface-2);}
 .ctxmenu button.danger{color:var(--danger);} .ctxmenu button.danger:hover{background:var(--danger-tint);}
 .ctxmenu .sep{height:1px;background:var(--border);margin:4px 6px;}

 /* Modals */
 .modal{position:fixed;inset:0;background:var(--overlay);backdrop-filter:blur(3px);display:none;justify-content:center;align-items:flex-start;
   padding:48px 16px;z-index:100;overflow:auto;}
 .modal.open{display:flex;}
 .mcard{background:var(--surface);border:1px solid var(--border);border-radius:14px;width:min(860px,100%);padding:18px 20px;box-shadow:var(--shadow);
   animation:fade .15s ease;}
 .mcard h2{margin:0 0 4px;font-size:15px;font-weight:600;color:var(--text);}
 .mcard .subtitle{color:var(--faint);font-size:12px;margin-bottom:12px;font-family:var(--mono);overflow-wrap:anywhere;}
 .mcard .section{margin-top:16px;}
 .mcard .section label{display:block;}
 .mcard .section h3,.panel h3{margin:0 0 8px;font-size:12px;font-weight:600;color:var(--muted);}
 .mcard pre{background:var(--inset);border:1px solid var(--border);border-radius:var(--radius-sm);padding:9px 11px;margin:0;color:var(--text);
   font-family:var(--mono);font-size:11.5px;overflow:auto;max-height:300px;white-space:pre-wrap;word-break:break-word;}
 .mcard .tcall{background:var(--surface-2);border-left:2px solid var(--info);border-radius:0 6px 6px 0;padding:5px 9px;margin-bottom:4px;font-size:11.5px;font-family:var(--mono);}
 .mcard .tcall .nm{color:var(--info);} .mcard .tcall .st{color:var(--ok);}
 .mcard .tcall.err{border-left-color:var(--danger);}
 .mcard .files a{display:block;color:var(--text);font-family:var(--mono);font-size:12px;padding:5px 9px;background:var(--surface-2);margin-bottom:3px;border-radius:6px;}
 .mcard .files a:hover{background:var(--accent-tint);color:var(--accent);text-decoration:none;}
 .mcard .actions,.panel .actions{display:flex;gap:6px;margin-top:14px;flex-wrap:wrap;}
 .perm{background:var(--warn-tint);border:1px solid transparent;padding:10px 12px;border-radius:var(--radius-sm);margin:6px 0;font-size:12px;}
 .perm .actions{margin-top:8px;}
 .qblock{margin:8px 0;padding:12px;border:1px solid var(--border);border-radius:var(--radius);background:var(--surface-2);}
 .close-x{float:right;cursor:pointer;color:var(--faint);font-size:20px;line-height:1;padding:2px 6px;border-radius:6px;}
 .close-x:hover{color:var(--text);background:var(--surface-2);}

 /* Panels */
 .panel{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:14px 16px;margin-bottom:10px;}
 details.panel > summary{cursor:pointer;list-style:none;}
 details.panel > summary::-webkit-details-marker{display:none;}
 details.panel > summary h3::after{content:" \25BE";color:var(--faint);}
 .row{display:flex;gap:8px;align-items:center;margin-bottom:8px;flex-wrap:wrap;}
 .row label{min-width:130px;}
 .row input,.row select{flex:1 1 240px;}
 .toggle{display:inline-flex;gap:6px;align-items:center;font-size:12px;color:var(--muted);cursor:pointer;}
 .toggle input{width:auto;accent-color:var(--accent);}
 .kv{display:grid;grid-template-columns:minmax(120px,200px) 1fr;gap:6px 14px;font-size:12px;}
 .kv .k{color:var(--muted);}
 .kv .v{color:var(--text);font-family:var(--mono);word-break:break-all;}
 pre.result,.pretty{background:var(--inset);border:1px solid var(--border);padding:10px 12px;border-radius:var(--radius-sm);max-height:420px;overflow:auto;
   font-family:var(--mono);font-size:11.5px;color:var(--text);white-space:pre-wrap;word-break:break-word;margin:0;}
 .mcp-row{display:flex;gap:10px;align-items:center;padding:9px 12px;background:var(--surface-2);border:1px solid transparent;border-radius:var(--radius-sm);
   margin-bottom:6px;flex-wrap:wrap;}
 .mcp-row .name{font-weight:600;color:var(--text);min-width:110px;}
 .mcp-row .type{color:var(--faint);font-family:var(--mono);font-size:11px;min-width:70px;}
 .mcp-row .url{color:var(--muted);font-family:var(--mono);font-size:11px;flex:1 1 180px;overflow-wrap:anywhere;}
 .mcp-row .st{min-width:80px;}

 /* Toasts */
 .toastwrap{position:fixed;bottom:16px;right:16px;left:16px;display:flex;flex-direction:column;align-items:flex-end;gap:6px;z-index:300;pointer-events:none;}
 .toast{pointer-events:auto;max-width:440px;background:var(--surface);border:1px solid var(--border);padding:9px 13px 9px 12px;border-radius:var(--radius);
   color:var(--text);font-size:12px;box-shadow:var(--shadow);animation:slideIn .18s ease;display:flex;gap:8px;align-items:baseline;}
 .toast::before{content:"";flex:none;width:7px;height:7px;border-radius:50%;background:var(--info);transform:translateY(-1px);}
 .toast.ok::before{background:var(--ok);} .toast.err::before{background:var(--danger);}
 @keyframes slideIn{from{transform:translateY(6px);opacity:0}to{transform:none;opacity:1}}

 /* Tool-call rows */
 .trow{display:grid;grid-template-columns:72px 1fr;align-items:stretch;border:1px solid var(--border);border-radius:var(--radius-sm);margin-bottom:4px;
   background:var(--surface);font-size:11.5px;overflow:hidden;}
 .trow.err{border-color:var(--danger-tint);box-shadow:inset 3px 0 0 var(--danger);}
 .trow .ts{color:var(--faint);font-family:var(--mono);background:var(--surface-2);padding:7px 6px;border-right:1px solid var(--border);text-align:center;
   font-size:10.5px;display:flex;align-items:center;justify-content:center;font-variant-numeric:tabular-nums;}
 .trow .body{padding:7px 10px;display:flex;flex-direction:column;gap:4px;min-width:0;}
 .trow .head{display:flex;align-items:center;gap:8px;flex-wrap:wrap;}
 .trow .tn{color:var(--text);font-family:var(--mono);font-size:11.5px;font-weight:600;}
 .trow .ts-pill{padding:1px 8px;border-radius:999px;font-size:10.5px;font-weight:500;color:var(--info);background:var(--info-tint);}
 .trow .ts-pill.completed{color:var(--ok);background:var(--ok-tint);}
 .trow .ts-pill.running{color:var(--info);background:var(--info-tint);}
 .trow .ts-pill.error{color:var(--danger);background:var(--danger-tint);}
 .trow .ts-pill.pending{color:var(--violet);background:var(--violet-tint);}
 .trow details{color:var(--muted);}
 .trow details summary{cursor:pointer;color:var(--muted);font-family:var(--mono);font-size:10.5px;}
 .trow details pre{background:var(--inset);padding:6px 8px;margin:4px 0 0;border-radius:6px;max-height:220px;overflow:auto;font-size:10.5px;
   white-space:pre-wrap;word-break:break-word;border:0;}
 .trow .err-text{color:var(--danger);margin-top:3px;}

 /* Narrow side panes */
 @media (max-width:600px){
  .wrap{padding:10px 10px 24px;}
  .card{padding:11px 12px 11px 14px;}
  .card .row1{flex-direction:column;align-items:stretch;gap:6px;}
  .card .sid{display:none;}
  .kv{grid-template-columns:1fr;gap:2px 0;} .kv .v{margin-bottom:6px;}
  .row label{min-width:0;flex:1 1 100%;}
  .modal{padding:12px 8px;} .mcard{padding:14px;border-radius:12px;}
  .trow{grid-template-columns:1fr;} .trow .ts{border-right:0;border-bottom:1px solid var(--border);justify-content:flex-start;padding:4px 10px;}
 }
</style></head><body>

<div class="wrap">
 <div class="top">
  <h1><span class="mark"><svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><circle cx="4" cy="4" r="1.6"/><circle cx="12" cy="4" r="1.6"/><circle cx="4" cy="12" r="1.6"/><circle cx="12" cy="12" r="1.6"/><path d="M5.6 4h4.8M4 5.6v4.8M12 5.6v4.8M5.6 12h4.8"/></svg></span>OpenCode Panel</h1>
  <div class="meta">
   <span class="clock" id="now"></span>
   <button class="fire-btn" onclick="openFire()">+ Fire worker</button>
  </div>
 </div>
 <div class="statusstrip" id="statusstrip"></div>

 <div class="tabs">
  <button class="tab-btn active" data-tab="workers" onclick="switchTab('workers')">Workers</button>
  <button class="tab-btn" data-tab="config" onclick="switchTab('config');loadConfig()">Config</button>
  <button class="tab-btn" data-tab="mcp" onclick="switchTab('mcp');loadMCP()">MCP</button>
  <button class="tab-btn" data-tab="tools" onclick="switchTab('tools')">Tools</button>
  <button class="tab-btn" data-tab="logs" onclick="switchTab('logs');loadLogs()">Logs</button>
 </div>

 <!-- Workers -->
 <div class="pane active" id="pane-workers">
  <div id="mcp-mini" class="meta" style="margin-bottom:8px"></div>
  <div id="alerts"></div>
  <div id="sessions"></div>
 </div>

 <!-- Config -->
 <div class="pane" id="pane-config">
  <div class="panel">
   <h3>OpenCode server</h3>
   <div id="serverstatus" class="kv"></div>
   <div class="actions" style="margin-top:10px;display:flex;gap:6px;flex-wrap:wrap">
    <button class="primary" onclick="svrAction('restart')">Restart</button>
    <button onclick="svrAction('start')">Start</button>
    <button class="danger" onclick="svrAction('stop')">Stop</button>
    <button onclick="loadConfig()">Refresh</button>
   </div>
  </div>
  <div class="panel">
   <h3>Opencode URL (this panel &rarr; opencode)</h3>
   <div class="row">
    <input id="ocurl" value="" />
    <button class="primary" onclick="setOcUrl()">Apply</button>
   </div>
  </div>
  <div class="panel">
   <h3>Discovered API key sources</h3>
   <div style="color:var(--muted);font-size:12px;margin-bottom:10px">
    The OpenCode key lives in one place: OpenCode's own auth.json, set with <code>opencode auth login</code>.
    Any key listed under ~/.claude.json is a stray copy that nothing needs.
   </div>
   <div id="discoveredkeys"></div>
  </div>
  <div class="panel">
   <h3>API keys / env (dashboard process env)</h3>
   <label class="toggle"><input type="checkbox" id="envshowraw" onchange="loadConfig()" /> show full values (dangerous)</label>
   <div id="envlist" class="kv" style="margin-top:8px"></div>
   <div style="margin-top:10px;color:var(--muted);font-size:11px">
    Set with the form below. Change takes effect for new OpenCode processes started from this panel (Config &rarr; Restart). For durable change, set it in your Windows user environment and restart.
   </div>
   <div class="row" style="margin-top:8px">
    <select id="envkey" style="flex:0 0 220px">
     <option>OPENCODE_API_KEY</option>
     <option>OPENAI_API_KEY</option>
     <option>ANTHROPIC_API_KEY</option>
     <option>GOOGLE_API_KEY</option>
     <option>DEEPSEEK_API_KEY</option>
     <option>MISTRAL_API_KEY</option>
     <option>GROQ_API_KEY</option>
     <option>HF_TOKEN</option>
     <option>GITHUB_TOKEN</option>
     <option>OPENROUTER_API_KEY</option>
     <option>CLOUDFLARE_API_TOKEN</option>
     <option>CLOUDFLARE_ACCOUNT_ID</option>
     <option>CLOUDFLARE_GATEWAY_ID</option>
    </select>
    <input id="envval" placeholder="(empty to unset)" />
    <button class="primary" onclick="setEnv()">Set</button>
   </div>
  </div>
  <details class="panel" ontoggle="if(this.open)loadProviders()">
   <summary style="cursor:pointer"><h3 style="display:inline">Providers configured</h3></summary>
   <label class="toggle" style="display:block;margin-top:10px"><input type="checkbox" id="provraw" onchange="renderProviders()" /> raw JSON</label>
   <div id="providers" style="margin-top:8px"></div>
  </details>
  <div class="panel">
   <h3>Filter</h3>
   <div class="row">
    <label>Title prefix</label>
    <input id="fltprefix" value="" placeholder='e.g. "Worker:" (empty = all)' />
    <button onclick="setFilter()">Apply</button>
   </div>
   <div class="row">
    <label>CWD contains</label>
    <input id="fltcwd" value="" placeholder="(empty = any)" />
    <button onclick="setFilter()">Apply</button>
   </div>
  </div>
 </div>

 <!-- MCP -->
 <div class="pane" id="pane-mcp">
  <div class="panel">
   <h3>Configured MCP servers (opencode config)</h3>
   <label class="toggle"><input type="checkbox" id="mcpcfgraw" onchange="renderMCP()" /> raw JSON</label>
   <div id="mcpconfigured" style="margin-top:8px"></div>
  </div>
  <div class="panel">
   <h3>Runtime status</h3>
   <label class="toggle"><input type="checkbox" id="mcprtraw" onchange="renderMCP()" /> raw JSON</label>
   <div id="mcpruntime" style="margin-top:8px"></div>
  </div>
  <div class="panel">
   <h3>Add MCP server</h3>
   <div class="row"><label>Name</label><input id="mcpname" placeholder="my-mcp" /></div>
   <div class="row"><label>Config JSON</label></div>
   <textarea id="mcpcfg" rows="5" placeholder='{"type":"remote","url":"http://127.0.0.1:3000/mcp"}'></textarea>
   <div class="actions"><button class="primary" onclick="addMCP()">Add</button></div>
  </div>
 </div>

 <!-- Tools (universal invoker) -->
 <div class="pane" id="pane-tools">
  <div class="panel">
   <h3>Universal OpenCode HTTP invoker</h3>
   <div class="row">
    <select id="iMethod" style="flex:0 0 100px">
     <option>GET</option><option>POST</option><option>DELETE</option><option>PUT</option>
    </select>
    <input id="iPath" list="pathcompl" placeholder="/session or /mcp or /permission ..." />
    <datalist id="pathcompl">
     <option value="/session"></option>
     <option value="/session/<id>"></option>
     <option value="/session/<id>/message?limit=50"></option>
     <option value="/session/<id>/abort"></option>
     <option value="/mcp"></option>
     <option value="/config"></option>
     <option value="/provider"></option>
     <option value="/permission"></option>
     <option value="/question"></option>
    </datalist>
    <button class="primary" onclick="invokeTool()">Send</button>
   </div>
   <div class="row"><label>Body (JSON, optional)</label></div>
   <textarea id="iBody" rows="5" placeholder='{"key": "value"}'></textarea>
   <div style="margin-top:10px">
    <label class="toggle"><input type="checkbox" id="irawtoggle" onchange="renderInvoke()" /> raw JSON</label>
    <div style="color:var(--muted);font-size:11px;margin:4px 0">Response:</div>
    <div id="iResult"></div>
   </div>
  </div>
  <div class="panel">
   <h3>Common recipes</h3>
   <div style="color:var(--muted);font-size:12px;line-height:1.6">
    <button onclick="setRecipe('GET','/session','')">List all sessions</button>
    <button onclick="setRecipe('GET','/permission','')">List pending permissions</button>
    <button onclick="setRecipe('GET','/mcp','')">MCP connector status</button>
    <button onclick="setRecipe('GET','/config','')">Full opencode config</button>
    <button onclick="setRecipe('GET','/provider','')">All providers &amp; models</button>
    <button onclick="setRecipe('POST','/session','{\&quot;title\&quot;:\&quot;probe\&quot;}')">Create session (title only)</button>
   </div>
  </div>
 </div>

 <!-- Logs -->
 <div class="pane" id="pane-logs">
  <div class="panel">
   <h3>Panel log (last 500 lines)</h3>
   <pre class="result" id="logbuf"></pre>
   <div class="actions"><button onclick="loadLogs()">Refresh</button></div>
  </div>
 </div>
</div>

<!-- Context menu -->
<div class="ctxmenu" id="ctxmenu">
 <button onclick="ctxAction('open')">Open details</button>
 <button onclick="ctxAction('reply')">Send message...</button>
 <div class="sep"></div>
 <button onclick="ctxAction('view-raw')">View raw session JSON</button>
 <button onclick="ctxAction('copy-id')">Copy session ID</button>
 <div class="sep"></div>
 <button class="danger" onclick="ctxAction('abort')">Abort</button>
</div>

<!-- Modals -->
<div class="modal" id="modal" onclick="if(event.target===this)closeModal()">
 <div class="mcard" id="mbody"></div>
</div>

<div class="modal" id="firemodal" onclick="if(event.target===this)closeFire()">
 <div class="mcard">
  <span class="close-x" onclick="closeFire()">&times;</span>
  <h2>Fire a new worker</h2>
  <div class="section"><label>Title <input id="ftitle" style="width:100%" placeholder="(auto-prefixed from model)" /></label></div>
  <div class="section">
   <label>Model</label>
   <div style="display:flex;gap:6px;align-items:center;margin-top:4px">
    <select id="fmodel" style="flex:1"></select>
    <button style="font-size:11px;padding:4px 8px" onclick="loadFireModels()">Refresh</button>
   </div>
  </div>
  <div class="section"><label>Prompt <textarea id="fprompt" rows="12" placeholder="The full worker prompt..."></textarea></label></div>
  <div class="actions">
   <button class="primary" onclick="doFire()">Fire</button>
   <button onclick="closeFire()">Cancel</button>
  </div>
 </div>
</div>

<div class="modal" id="rawmodal" onclick="if(event.target===this)closeRaw()">
 <div class="mcard">
  <span class="close-x" onclick="closeRaw()">&times;</span>
  <h2 id="rawtitle">raw</h2>
  <pre class="result" id="rawbody"></pre>
 </div>
</div>

<div class="toastwrap" id="toasts"></div>

<script>
let state = {sessions:[],mcp_runtime:{},mcp_configured:{},orphan_permissions:[],orphan_questions:[],config:{}};
let openModalSid = null;
let ctxTargetSid = null;
let providersData = null;
let lastInvokeResult = null;

function fmt(n){n=Number(n)||0;if(n>=1e6)return (n/1e6).toFixed(2)+'M';if(n>=1e3)return (n/1e3).toFixed(1)+'k';return String(n);}
function age(ms){if(ms<1000)return ms+'ms';let s=ms/1000;if(s<60)return s.toFixed(1)+'s';let m=s/60;if(m<60)return m.toFixed(1)+'m';return (m/60).toFixed(1)+'h';}
function escH(s){return String(s||'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function fmtTs(ms){if(!ms)return '';const d=new Date(ms);return d.toLocaleTimeString('en-GB',{hour12:false});}
function uptimeFmt(sec){if(sec==null)return '?';if(sec<60)return sec+'s';if(sec<3600)return Math.floor(sec/60)+'m '+(sec%60)+'s';const h=Math.floor(sec/3600);return h+'h '+Math.floor((sec%3600)/60)+'m';}

// Toasts --------------------------------------------------------------------
function toast(msg, kind){
  const wrap = document.getElementById('toasts');
  const el = document.createElement('div');
  el.className = 'toast '+(kind||'');
  el.textContent = msg;
  wrap.appendChild(el);
  setTimeout(()=>{el.style.opacity='0';el.style.transition='opacity .3s';setTimeout(()=>el.remove(),300);}, 4500);
}
async function callApi(method, url, body){
  try{
    const opts = {method, headers:{'Content-Type':'application/json'}};
    if(body) opts.body = JSON.stringify(body);
    const r = await fetch(url, opts);
    const data = await r.json().catch(()=>({__nonjson__:true}));
    if(!r.ok){ toast('ERR '+r.status+': '+(data.error||JSON.stringify(data).slice(0,120)), 'err'); return {ok:false,data}; }
    return {ok:true, data};
  }catch(e){ toast('fetch error: '+e, 'err'); return {ok:false, error:e}; }
}

function switchTab(name){
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.toggle('active', b.dataset.tab===name));
  document.querySelectorAll('.pane').forEach(p=>p.classList.toggle('active', p.id==='pane-'+name));
}

// Workers rendering ---------------------------------------------------------

let adminCache = null;
async function refreshAdmin(){
  const r = await fetch(document.getElementById('envshowraw').checked ? '/api/admin?raw=1' : '/api/admin');
  adminCache = await r.json();
  renderStatusStrip();
  if(document.getElementById('pane-config').classList.contains('active')) renderConfig();
}
setInterval(refreshAdmin, 10000);

function renderStatusStrip(){
  const el = document.getElementById('statusstrip');
  if(!adminCache){
    el.innerHTML = '<div class="cell"><div class="lbl">Status</div><div class="val">loading</div></div>';
    return;
  }
  const a = adminCache;
  const alive = a.alive;
  const up = (a.uptime||{}).uptime_sec;
  const health = a.api_health || {};

  // Unified "API KEY" concept: one of OK / NEEDS-SYNC / MISSING.
  let keyState, keyLabel, keyAction = '';
  if(health.connected){ keyState='ok'; keyLabel='connected'; }
  else if(health.ok===false && !alive){ keyState='warn'; keyLabel='server not reachable'; }
  else { keyState='bad'; keyLabel='not connected - run opencode auth login'; }

  const h = (state && state.hidden) || {};
  const shown = (state && state.sessions) ? state.sessions.length : 0;
  const hiddenParts = [];
  if(h.by_filter) hiddenParts.push(`${h.by_filter} by filter`);
  if(h.by_cwd) hiddenParts.push(`${h.by_cwd} by cwd`);
  if(h.as_idle) hiddenParts.push(`${h.as_idle} idle`);
  const hiddenTxt = hiddenParts.length ? ` <span style="color:var(--muted)">(${hiddenParts.join(', ')} hidden)</span>` : '';

  const cells = [
    `<div class="cell ${alive?'ok':'bad'}"><div class="lbl">OpenCode</div><div class="val">${alive?'running':'down'}</div></div>`,
    `<div class="cell"><div class="lbl">Uptime</div><div class="val">${uptimeFmt(up)}</div></div>`,
    `<div class="cell"><div class="lbl">PIDs</div><div class="val">${(a.processes||[]).join(',')||'-'}</div></div>`,
    `<div class="cell ${keyState}"><div class="lbl">Provider</div><div class="val">${escH(keyLabel)}${keyAction}</div></div>`,
    `<div class="cell"><div class="lbl">Providers</div><div class="val">${health.providers_total||'?'}</div></div>`,
    `<div class="cell"><div class="lbl">Sessions</div><div class="val">${shown}${hiddenTxt}</div></div>`,
    `<div class="cell"><div class="lbl">URL</div><div class="val">${escH(a.opencode_url||'?')}</div></div>`,
  ];
  el.innerHTML = cells.join('');
}

function render(){
  renderStatusStrip();
  document.getElementById('now').textContent = new Date().toLocaleTimeString();

  const mini = document.getElementById('mcp-mini');
  const parts = [];
  for(const [n,info] of Object.entries(state.mcp_runtime||{})){
    if(n.startsWith('__'))continue;
    const st=(info&&info.status)||'?'; const cls=st==='connected'?'ok':'bad';
    parts.push(`${escH(n)} <span class="${cls}">${escH(st)}</span>`);
  }
  mini.innerHTML = 'MCP &nbsp;' + (parts.join(' &middot; ')||'<span style="color:var(--muted)">(none)</span>');

  const alerts = document.getElementById('alerts');
  let a=''; const op=state.orphan_permissions||[]; const oq=state.orphan_questions||[];
  if(op.length||oq.length){
    a += `<div class="alert"><strong>Orphan pending items</strong>:<ul>`;
    for(const p of op) a += `<li>permission <code>${escH(p.id)}</code> session <code>${escH(p.sessionID)}</code> ${escH(p.permission||p.type||'')}</li>`;
    for(const q of oq) a += `<li>question <code>${escH(q.id||q.requestID)}</code> session <code>${escH(q.sessionID)}</code></li>`;
    a += `</ul></div>`;
  }
  alerts.innerHTML = a;

  const cont = document.getElementById('sessions');
  const list = state.sessions||[];
  if(!list.length){cont.innerHTML='<div class="empty">No active workers. Fire one with the button above.</div>';return;}
  cont.innerHTML = list.map(renderCard).join('');
  cont.querySelectorAll('.card').forEach(c=>{
    c.addEventListener('contextmenu', e=>{e.preventDefault(); openCtx(e, c.dataset.sid);});
  });
  if(openModalSid){
    const s = list.find(x=>x.id===openModalSid);
    if(s) renderModal(s);
  }
  // Auto-pop modal on edge-trigger: session went from no pending items -> any pending item.
  // We only auto-open when the modal isn't already showing, and we honor the user's dismissal
  // by requiring a 0->N transition for THIS session before re-opening.
  if(!openModalSid){
    for(const s of list){
      const pending = (s.pending_permissions||[]).length + (s.pending_questions||[]).length;
      const prev = lastPending[s.id]||0;
      if(pending>0 && prev===0){
        openModal(s.id);
        break; // only one at a time
      }
    }
  }
  for(const s of list){
    lastPending[s.id] = (s.pending_permissions||[]).length + (s.pending_questions||[]).length;
  }
}
let lastPending = {};

function statusExplain(s){
  if(s.status==='retry' && s.stage==='interrupted')
    return 'Last assistant message is open but no new tokens in ' + age(s.age_ms) + '. The session was likely cut off by a provider error (free-tier quota, network, server restart). Click Retry to re-poke it, or Abort to end it.';
  if(s.status==='input_required')
    return 'Worker is paused awaiting a decision. Open details to approve/reject.';
  if(s.status==='accepted') return 'Queued at opencode; waiting for a worker slot.';
  if(s.status==='retry') return 'Provider returned an error; opencode is retrying automatically.';
  if(s.status==='error') return 'Last turn ended with ' + s.stage + '. Click Retry to re-poke it.';
  if(s.stage==='stalled') return 'Running but no new activity in >60s.';
  return '';
}

function renderCard(s){
  const cls = s.status + (s.age_ms>60000 && s.status==='running' ? ' stalled':'');
  const sid = escH(s.id.slice(0,28));
  const t = s.tokens||{};
  const canRetry = s.status==='retry' || s.status==='failed' || s.status==='error';
  const explain = statusExplain(s);
  return `<div class="card ${cls}" data-sid="${s.id}" onclick="openModal('${s.id}')">
   <div class="row1">
    <div class="title">${escH(s.title)}</div>
    <div style="display:flex;gap:8px;align-items:center" onclick="event.stopPropagation()">
     <span class="status ${s.status}">${escH(String(s.status).replace(/_/g,' '))}${s.stage?' &middot; '+escH(s.stage):''}</span>
     <span class="sid">${sid}</span>
    </div>
   </div>
   ${explain ? `<div class="explain">${escH(explain)}</div>` : ''}
   <div class="meta2">
    <span><code>${escH(s.model_provider)}/${escH(s.model_id)}</code></span>
    <span><code>${escH(s.directory||'')}</code></span>
    <span style="color:${s.status==='running'&&s.age_ms>60000?'var(--danger)':'var(--muted)'}">${age(s.age_ms)}</span>
    ${s.written_files.length?`<span>wrote <strong style="color:var(--ok)">${s.written_files.length}</strong> file${s.written_files.length===1?'':'s'}</span>`:''}
   </div>
   <div class="tok">
    <span>in <span class="in">${fmt(t.input)}</span></span>
    <span>out <span class="out">${fmt(t.output)}</span></span>
    <span>reasoning <span class="rsn">${fmt(t.reasoning)}</span></span>
    <span>cache <span class="in">${fmt(t.cache_read)}</span></span>
   </div>
   <div class="cardactions" onclick="event.stopPropagation()">
    <button class="good" ${canRetry?'':'disabled'} onclick="retryWorker('${s.id}')">Retry</button>
    <button onclick="openModal('${s.id}');setTimeout(openReply,80)">Reply</button>
    <button class="danger" onclick="abort('${s.id}')">Abort</button>
   </div>
  </div>`;
}

function renderModal(s){
  const mb = document.getElementById('mbody');
  const perms=s.pending_permissions||[], qs=s.pending_questions||[];
  const permHtml = perms.map(p=>{
    const pat=((p.metadata||{}).patterns||[]).join(', ')||((p.metadata||{}).command||'');
    return `<div class="perm"><strong>${escH(p.permission||p.type||'?')}</strong> <code>${escH(p.id)}</code>
     <div style="color:var(--text);margin-top:3px">${escH(pat)}</div>
     <div class="actions"><button class="good" onclick="approve('${s.id}','${p.id}','always')">Approve always</button>
     <button class="primary" onclick="approve('${s.id}','${p.id}','once')">Approve once</button>
     <button class="danger" onclick="approve('${s.id}','${p.id}','reject')">Reject</button></div></div>`;
  }).join('');
  const qHtml = qs.map(req=>{
    const reqId = escH(req.id || req.requestID || '');
    // A single `request` can carry 1-N question objects; opencode wants one answer array per question.
    const questions = Array.isArray(req.questions) ? req.questions : [];
    const inner = questions.map((q, idx) => {
      const header = escH(q.header || '');
      const qtext = escH(q.question || '');
      const opts = (q.options || []).map(o => {
        const label = o.label || '';
        const desc = escH(o.description || '');
        const safeLabel = label.replace(/'/g, "\\'").replace(/"/g, '&quot;');
        return `<button class="primary" style="display:block;width:100%;text-align:left;margin:4px 0;padding:8px 10px" onclick="answerQuestion('${reqId}',${idx},'${escH(safeLabel)}')"><strong>${escH(label)}</strong>${desc?`<div style="color:var(--muted);font-size:11px;margin-top:3px;font-weight:normal">${desc}</div>`:''}</button>`;
      }).join('');
      return `<div class="qblock" style="margin:8px 0;padding:10px;border:1px solid var(--border);border-radius:4px;background:var(--surface-2)">
        ${header?`<div style="color:var(--warn);font-size:11px;letter-spacing:.5px;text-transform:uppercase;margin-bottom:4px">${header}</div>`:''}
        <div style="margin-bottom:8px">${qtext}</div>
        ${opts}
        <div style="margin-top:8px;display:flex;gap:6px;align-items:center">
          <input type="text" id="qfree_${reqId}_${idx}" placeholder="Or type a custom answer..." style="flex:1;padding:6px 8px;background:var(--inset);border:1px solid var(--border);color:var(--text);border-radius:3px;font-size:12px"/>
          <button onclick="answerQuestionFree('${reqId}',${idx})">Send</button>
        </div>
      </div>`;
    }).join('');
    return `<div class="qreq" data-reqid="${reqId}">
      <div style="color:var(--muted);font-size:10px;margin-bottom:4px">question id <code>${reqId}</code></div>
      ${inner || '<pre>'+escH(JSON.stringify(req,null,2))+'</pre>'}
    </div>`;
  }).join('');
  const toolHtml = (s.tool_tail||[]).map(t=>{
    const ts = fmtTs(t.started);
    const stCls = t.err?'error':(t.status||'pending');
    return `<div class="trow ${t.err?'err':''}">
     <div class="ts">${escH(ts||'')}</div>
     <div class="body">
      <div class="head">
       <span class="tn">${escH(t.tool||'')}</span>
       <span class="ts-pill ${stCls}">${escH(t.status||'?')}</span>
      </div>
      ${t.input_preview?`<details><summary>input</summary><pre>${escH(t.input_preview)}</pre></details>`:''}
      ${t.output_preview?`<details><summary>output</summary><pre>${escH(t.output_preview)}</pre></details>`:''}
      ${t.err?`<div class="err-text">error: ${escH(t.err)}</div>`:''}
     </div>
    </div>`;
  }).join('') || '<div style="color:var(--faint)">(no tool calls yet)</div>';
  const files = (s.written_files||[]);
  const filesHtml = files.length
    ? files.map(f=>`<a href="/view?path=${encodeURIComponent(f)}" target="_blank" rel="noopener">${escH(f)}</a>`).join('')
    : '<div style="color:var(--faint)">(none)</div>';
  const text = s.text_tail || '';
  mb.innerHTML = `<span class="close-x" onclick="closeModal()">&times;</span>
   <h2>${escH(s.title)}</h2>
   <div class="subtitle">${escH(s.id)} &middot; ${escH(s.status)}${s.stage?' &middot; '+escH(s.stage):''} &middot; age ${age(s.age_ms)}</div>
   ${perms.length?`<div class="section"><h3>Pending permissions</h3>${permHtml}</div>`:''}
   ${qs.length?`<div class="section"><h3>Pending questions</h3>${qHtml}</div>`:''}
   <div class="section"><h3>Written files</h3><div class="files">${filesHtml}</div></div>
   <div class="section"><h3>Recent tool calls (last 10)</h3>${toolHtml}</div>
   <div class="section"><h3>Last text output</h3><pre>${escH(text) || '<i>(none)</i>'}</pre></div>
   <div class="actions"><button class="primary" onclick="openReply('${s.id}')">Reply...</button>
     <button class="danger" onclick="abort('${s.id}')">Abort</button>
     <button onclick="viewRawSession('${s.id}')">Raw JSON</button></div>
   <div id="replybox" style="display:none;margin-top:12px">
     <textarea id="replytxt" rows="6" placeholder="Follow-up message..."></textarea>
     <div class="actions"><button class="primary" onclick="doReply('${s.id}')">Send</button></div>
   </div>`;
}

function openModal(sid){openModalSid=sid; const s=state.sessions.find(x=>x.id===sid); if(!s)return; renderModal(s); document.getElementById('modal').classList.add('open');}
function closeModal(){openModalSid=null; document.getElementById('modal').classList.remove('open');}
function openReply(){document.getElementById('replybox').style.display='block';}
function openFire(){document.getElementById('firemodal').classList.add('open');loadFireModels();}
let fireModelsLoaded=false;
async function loadFireModels(){
  const sel=document.getElementById('fmodel');
  const prev=sel.value;
  sel.innerHTML='<option value="">loading...</option>';
  try{
    const r=await fetch('/api/models');
    const d=await r.json();
    const models=d.models||[];
    sel.innerHTML='';
    const grouped={};
    for(const m of models){(grouped[m.provider]=grouped[m.provider]||[]).push(m);}
    for(const [prov,ms] of Object.entries(grouped).sort(([a],[b])=>a.localeCompare(b))){
      const og=document.createElement('optgroup');
      og.label=prov;
      for(const m of ms){
        const o=document.createElement('option');
        o.value=prov+'/'+m.id;
        o.textContent=m.name||m.id;
        og.appendChild(o);
      }
      sel.appendChild(og);
    }
    if(prev&&sel.querySelector('option[value="'+prev+'"]'))sel.value=prev;
    else{const bp=sel.querySelector('option[value="opencode/big-pickle"]');if(bp)sel.value=bp.value;}
    fireModelsLoaded=true;
  }catch(e){sel.innerHTML='<option value="">error loading models</option>';toast('model list: '+e,'err');}
}
function closeFire(){document.getElementById('firemodal').classList.remove('open');}
function closeRaw(){document.getElementById('rawmodal').classList.remove('open');}

// Context menu --------------------------------------------------------------

function openCtx(e, sid){
  ctxTargetSid = sid;
  const m = document.getElementById('ctxmenu');
  m.style.left = e.pageX + 'px';
  m.style.top = e.pageY + 'px';
  m.classList.add('open');
}
document.addEventListener('click', (e)=>{
  const m = document.getElementById('ctxmenu');
  if(!m.contains(e.target)) m.classList.remove('open');
});
async function ctxAction(action){
  document.getElementById('ctxmenu').classList.remove('open');
  const sid = ctxTargetSid; if(!sid)return;
  if(action==='open') return openModal(sid);
  if(action==='reply'){ openModal(sid); setTimeout(()=>openReply(sid),50); return; }
  if(action==='abort') return abort(sid);
  if(action==='copy-id'){ navigator.clipboard.writeText(sid); return; }
  if(action==='view-raw') return viewRawSession(sid);
}

async function viewRawSession(sid){
  const r = await fetch('/api/invoke', {method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({method:'GET', path:'/session/'+sid})});
  const data = await r.json();
  document.getElementById('rawtitle').textContent = 'raw session: ' + sid;
  document.getElementById('rawbody').textContent = JSON.stringify(data.body, null, 2);
  document.getElementById('rawmodal').classList.add('open');
}

async function abort(sid){
  // No native confirm - Claude Code's browser pane blocks it. Just do it;
  // optimistically HIDE the card and toast so the operator sees instant effect.
  const c = document.querySelector('.card[data-sid="'+sid+'"]');
  if(c){ c.style.opacity='0.3'; c.style.pointerEvents='none'; c.querySelector('.title')?.insertAdjacentHTML('beforeend', ' <span style="color:var(--danger);font-size:11px">[ABORTING]</span>'); }
  const r = await callApi('POST', '/api/abort/'+sid);
  if(r.ok){
    const notes = r.data.notes||[];
    toast('abort: ' + (notes.join('; ')||'ok') + ' - ' + sid.slice(0,24), notes.length?'err':'ok');
    // Hide card; the next SSE snapshot will or won't bring it back depending on the active filter.
    if(c) c.style.display='none';
  } else {
    // Rollback optimistic change
    if(c){ c.style.opacity=''; c.style.pointerEvents=''; }
  }
}
async function doReply(sid){
  const t = document.getElementById('replytxt').value; if(!t.trim()){toast('message empty', 'err');return;}
  const r = await callApi('POST', '/api/reply/'+sid, {prompt:t});
  if(r.ok){
    toast('message sent ('+t.length+' chars) to '+sid.slice(0,20), 'ok');
    document.getElementById('replytxt').value='';
    document.getElementById('replybox').style.display='none';
  }
}
async function approve(sid,pid,reply){
  const r = await callApi('POST', '/api/permission/'+sid+'/'+pid, {reply});
  if(r.ok) toast('permission '+reply+' for '+pid.slice(0,16), 'ok');
}
async function answerQuestion(reqId, qIdx, label){
  // Build an answers array with the chosen label in slot qIdx and empty strings elsewhere.
  // Most question requests carry a single question (qIdx=0) so this is just [[label]].
  const req = findQuestionReq(reqId);
  const n = req && Array.isArray(req.questions) ? req.questions.length : 1;
  const answers = [];
  for(let i=0;i<n;i++) answers.push(i===qIdx?[label]:['']);
  const r = await callApi('POST', '/api/question/'+reqId+'/reply', {answers});
  if(r.ok) toast('answered: '+label.slice(0,30), 'ok');
  else toast('answer failed: '+(r.data&&r.data.error||'?'), 'err');
}
async function answerQuestionFree(reqId, qIdx){
  const input = document.getElementById('qfree_'+reqId+'_'+qIdx);
  const text = input ? input.value.trim() : '';
  if(!text){ toast('answer empty', 'err'); return; }
  await answerQuestion(reqId, qIdx, text);
  if(input) input.value='';
}
function findQuestionReq(reqId){
  for(const s of (state.sessions||[])){
    for(const req of (s.pending_questions||[])){
      if((req.id||req.requestID)===reqId) return req;
    }
  }
  for(const req of (state.orphan_questions||[])){
    if((req.id||req.requestID)===reqId) return req;
  }
  return null;
}
async function retryWorker(sid){
  // No native retry; nudge the session with an empty continuation.
  const r = await callApi('POST', '/api/reply/'+sid, {prompt: 'Please continue with your prior instructions.'});
  if(r.ok) toast('retry nudge sent to '+sid.slice(0,20), 'ok');
}
async function doFire(){
  const title=document.getElementById('ftitle').value.trim();
  const prompt=document.getElementById('fprompt').value;
  if(!title||!prompt){toast('title and prompt required', 'err');return;}
  const mv=document.getElementById('fmodel').value;
  const [provider,model]=mv.includes('/')?[mv.split('/')[0],mv.split('/').slice(1).join('/')]:[null,null];
  const body={title,prompt};
  if(provider)body.provider=provider;
  if(model)body.model=model;
  const r = await callApi('POST', '/api/fire', body);
  if(r.ok){
    toast('fired: '+(r.data.sessionID||'?').slice(0,20)+' on '+(model||'big-pickle'), 'ok');
    closeFire();
    document.getElementById('ftitle').value=''; document.getElementById('fprompt').value='';
  }
}

// Config tab ----------------------------------------------------------------

async function loadConfig(){
  await refreshAdmin();
  renderConfig();
}
function renderConfig(){
  const d = adminCache; if(!d) return;
  document.getElementById('ocurl').value = d.opencode_url;
  document.getElementById('fltprefix').value = d.filter_prefix;
  document.getElementById('fltcwd').value = d.cwd_filter;
  const up = d.uptime || {};
  const ss = document.getElementById('serverstatus');
  ss.innerHTML = `
    <div class="k">URL</div><div class="v">${escH(d.opencode_url)}</div>
    <div class="k">Alive</div><div class="v ${d.alive?'ok':'bad'}">${d.alive?'yes':'no'}</div>
    <div class="k">Processes</div><div class="v">${(d.processes||[]).join(', ')||'(none)'}</div>
    <div class="k">Uptime</div><div class="v">${uptimeFmt(up.uptime_sec)}</div>
    <div class="k">Started</div><div class="v">${escH(up.start_time||'')}</div>
    <div class="k">Providers available</div><div class="v">${(d.api_health||{}).providers_total||'?'}</div>
    <div class="k">opencode models</div><div class="v">${((d.api_health||{}).opencode_models||[]).join(', ')||'?'}</div>`;
  // Discovered keys
  const dk = d.discovered_keys || {};
  const dkEl = document.getElementById('discoveredkeys');
  if(Object.keys(dk).length){
    dkEl.innerHTML = Object.entries(dk).map(([src,keys])=>{
      const rows = Object.entries(keys).map(([k,v])=>`<div class="k">${escH(k)}</div><div class="v" style="color:var(--accent)">${escH(v)}</div>`).join('');
      return `<div style="margin-bottom:10px"><div style="color:var(--accent);font-weight:600;font-size:12px;margin-bottom:4px">${escH(src)}</div><div class="kv">${rows}</div></div>`;
    }).join('');
  } else {
    dkEl.innerHTML = '<div style="color:var(--faint)">(no keys discovered in config files)</div>';
  }
  // Env
  const env = d.env || {};
  const show = document.getElementById('envshowraw').checked;
  const envHtml = Object.entries(env).map(([k,v])=>{
    const vd = v===null?'<i style="color:var(--faint)">unset</i>':(show?escH((d.env_raw||{})[k]||v):escH(v));
    const col = v===null?'var(--faint)':'var(--accent)';
    return `<div class="k">${escH(k)}</div><div class="v" style="color:${col}">${vd}</div>`;
  }).join('');
  document.getElementById('envlist').innerHTML = envHtml;
}

let providerFilter = '';
let expandedProvider = null;
async function loadProviders(){
  document.getElementById('providers').innerHTML = '<div style="color:var(--muted)">loading...</div>';
  const r = await callApi('GET', '/api/providers');
  providersData = r.ok ? r.data.providers : null;
  renderProviders();
}
function renderProviders(){
  const pd = providersData; if(!pd){document.getElementById('providers').innerHTML='<div style="color:var(--faint)">(provider list unavailable)</div>';return;}
  if(document.getElementById('provraw').checked){
    document.getElementById('providers').innerHTML = `<pre class="result">${escH(JSON.stringify(pd,null,2))}</pre>`;
    return;
  }
  const all = (pd.all)||[];
  const filt = providerFilter.toLowerCase();
  const matched = filt ? all.filter(p => (p.id+' '+p.name).toLowerCase().includes(filt)) : all;
  const rows = matched.map(p=>{
    const envs = (p.env||[]).join(', ') || '(none)';
    const nmodels = Object.keys(p.models||{}).length;
    const expanded = expandedProvider === p.id;
    let modelsHtml = '';
    if(expanded){
      const models = Object.values(p.models||{});
      modelsHtml = `<div style="margin-top:6px;padding:6px 8px;background:var(--inset);border-radius:3px;max-height:220px;overflow:auto">
       ${models.map(m=>{
         const caps = Object.entries(m.capabilities||{}).filter(([k,v])=>v===true || (typeof v==='object' && Object.values(v||{}).some(x=>x))).map(([k,v])=>k).slice(0,8).join(', ');
         return `<div style="padding:3px 0;border-bottom:1px solid var(--surface-2);font-size:11px"><strong style="color:var(--accent)">${escH(m.id)}</strong> <span style="color:var(--muted)">${escH(m.name||'')}</span> ${caps?`<span style="color:var(--faint)">&mdash; ${escH(caps)}</span>`:''}</div>`;
       }).join('')||'<div style="color:var(--faint)">(no models)</div>'}
      </div>`;
    }
    return `<div style="padding:6px 8px;border:1px solid var(--border);border-radius:3px;margin-bottom:4px;background:var(--surface-2);cursor:pointer"
             onclick="toggleProvider('${escH(p.id)}')">
      <div style="display:flex;justify-content:space-between;gap:8px"><span><strong>${escH(p.name)}</strong> <span style="color:var(--muted)">(${escH(p.id)})</span></span>
       <span style="color:var(--muted);font-size:11px">${nmodels} models ${expanded?'&#9650;':'&#9660;'}</span></div>
      <div style="color:var(--muted);font-size:11px">env: ${escH(envs)}</div>
      ${modelsHtml}
    </div>`;
  }).join('');
  document.getElementById('providers').innerHTML =
    `<div style="margin-bottom:8px;display:flex;gap:8px;align-items:center">
       <input placeholder="filter providers..." value="${escH(providerFilter)}" style="flex:1;max-width:300px" oninput="providerFilter=this.value;renderProviders()" />
       <span style="color:var(--muted);font-size:11px">${matched.length} / ${all.length} providers</span>
     </div>
     <div style="max-height:420px;overflow-y:auto;border:1px solid var(--border);border-radius:3px;padding:4px;background:var(--inset)">${rows||'<div style="color:var(--faint);padding:8px">no matches</div>'}</div>`;
}
function toggleProvider(pid){
  expandedProvider = (expandedProvider === pid) ? null : pid;
  renderProviders();
}

async function setOcUrl(){
  const v = document.getElementById('ocurl').value.trim();
  await fetch('/api/admin',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({opencode_url:v})});
  loadConfig();
}
async function setFilter(){
  const prefix = document.getElementById('fltprefix').value;
  const cwd = document.getElementById('fltcwd').value;
  await fetch('/api/admin',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({filter_prefix:prefix, cwd_filter:cwd})});
  loadConfig();
}
async function setEnv(){
  const k = document.getElementById('envkey').value;
  const v = document.getElementById('envval').value;
  await fetch('/api/env',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({key:k, value:v})});
  document.getElementById('envval').value = '';
  loadConfig();
}
async function svrAction(action){
  toast(action+'ing opencode...', 'ok');
  const btns = document.querySelectorAll('#pane-config button'); btns.forEach(b=>b.disabled=true);
  const r = await callApi('POST', '/api/server/'+action);
  btns.forEach(b=>b.disabled=false);
  if(r.ok) toast('server '+action+': ' + JSON.stringify(r.data).slice(0,80), 'ok');
  for(let i=0; i<10; i++){ await new Promise(res=>setTimeout(res, 1500)); await refreshAdmin(); if(adminCache && adminCache.alive) break; }
}

// MCP tab -------------------------------------------------------------------

async function loadMCP(){ renderMCP(); }

function renderMCP(){
  const cfg = state.mcp_configured||{};
  const rt = state.mcp_runtime||{};
  const el1 = document.getElementById('mcpconfigured');
  const el2 = document.getElementById('mcpruntime');
  if(document.getElementById('mcpcfgraw').checked){
    el1.innerHTML = `<pre class="result">${escH(JSON.stringify(cfg,null,2))}</pre>`;
  } else {
    el1.innerHTML = Object.entries(cfg).map(([n,c])=>{
      const t = (c&&c.type)||'?'; const u = (c&&(c.url||c.command))||'';
      return `<div class="mcp-row">
       <span class="name">${escH(n)}</span>
       <span class="type">${escH(t)}</span>
       <span class="url">${escH(u)}</span>
       <button class="danger" onclick="removeMCP('${escH(n)}')">Remove</button>
      </div>`;
    }).join('') || '<div style="color:var(--faint)">(none configured)</div>';
  }
  if(document.getElementById('mcprtraw').checked){
    el2.innerHTML = `<pre class="result">${escH(JSON.stringify(rt,null,2))}</pre>`;
  } else {
    el2.innerHTML = Object.entries(rt).map(([n,v])=>{
      const st = (v&&v.status)||'?'; const cls = st==='connected'?'ok':'bad';
      return `<div class="mcp-row">
       <span class="name">${escH(n)}</span>
       <span class="st ${cls}">${escH(st)}</span>
      </div>`;
    }).join('') || '<div style="color:var(--faint)">(none)</div>';
  }
}

async function addMCP(){
  const name = document.getElementById('mcpname').value.trim();
  const cfg = document.getElementById('mcpcfg').value;
  if(!name || !cfg){toast('name + config required', 'err');return;}
  let parsed; try{parsed=JSON.parse(cfg);}catch(e){toast('invalid JSON: '+e, 'err');return;}
  const r = await callApi('POST', '/api/invoke', {method:'POST', path:'/mcp', body:{name, config:parsed}});
  if(r.ok){
    toast('MCP '+name+' added: ' + JSON.stringify(r.data.body||'(ok)').slice(0,100), 'ok');
    document.getElementById('mcpname').value=''; document.getElementById('mcpcfg').value='';
  }
}
async function removeMCP(name){
  toast('removing MCP '+name+'...', 'ok');
  const r = await callApi('POST', '/api/invoke', {method:'DELETE', path:'/mcp/'+name});
  if(r.ok) toast('MCP '+name+' removed: ' + JSON.stringify(r.data.body||'(ok)').slice(0,100), 'ok');
}

// Tools tab (universal invoker) --------------------------------------------

function setRecipe(method,path,body){
  document.getElementById('iMethod').value = method;
  document.getElementById('iPath').value = path;
  document.getElementById('iBody').value = body;
}
async function invokeTool(){
  const method = document.getElementById('iMethod').value;
  const path = document.getElementById('iPath').value.trim();
  const bodyText = document.getElementById('iBody').value.trim();
  let body = null;
  if(bodyText){ try{body=JSON.parse(bodyText);}catch(e){toast('body not valid JSON: '+e, 'err');return;} }
  const r = await fetch('/api/invoke',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({method, path, body})});
  lastInvokeResult = await r.json();
  renderInvoke();
}
function renderInvoke(){
  const el = document.getElementById('iResult');
  if(!lastInvokeResult){el.innerHTML='';return;}
  const raw = document.getElementById('irawtoggle').checked;
  if(raw){
    el.innerHTML = `<pre class="result">${escH(JSON.stringify(lastInvokeResult,null,2))}</pre>`;
  } else {
    const b = lastInvokeResult.body;
    let prettyBody;
    if(b==null){prettyBody='<i style="color:var(--faint)">(no body)</i>';}
    else if(typeof b==='object'){prettyBody = `<pre class="pretty">${escH(JSON.stringify(b,null,2))}</pre>`;}
    else {prettyBody = `<pre class="pretty">${escH(String(b))}</pre>`;}
    el.innerHTML = `<div style="color:var(--muted);font-size:11px;margin-bottom:4px">HTTP ${lastInvokeResult.status||'?'} &middot; url: <code>${escH(lastInvokeResult.url||'')}</code></div>` + prettyBody;
  }
}

// Logs ----------------------------------------------------------------------

async function loadLogs(){
  const r = await fetch('/api/logs');
  const d = await r.json();
  document.getElementById('logbuf').textContent = d.lines.join('\n');
}

// SSE -----------------------------------------------------------------------

const es = new EventSource('/api/stream');
es.addEventListener('snapshot', e=>{ state=JSON.parse(e.data); render(); if(document.getElementById('pane-mcp').classList.contains('active')) renderMCP(); });
es.addEventListener('tick', e=>{});
es.addEventListener('err', e=>console.error('server err', e.data));

// First paint - fire admin fetch immediately so the status strip populates
// without waiting for the 10s interval.
refreshAdmin();
fetch('/api/snapshot').then(r=>r.json()).then(d=>{state=d; render();});
setInterval(()=>document.getElementById('now').textContent=new Date().toLocaleTimeString(), 1000);
</script>
</body></html>
"""


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    def _json(self, obj, status=200):
        body = json.dumps(obj, default=str).encode("utf-8")
        self.send_response(status); self.send_header("Content-Type","application/json")
        self.send_header("Cache-Control","no-store"); self.send_header("Content-Length",str(len(body)))
        self.end_headers(); self.wfile.write(body)

    def _text(self, text, ctype="text/plain", status=200):
        body = text.encode("utf-8")
        self.send_response(status); self.send_header("Content-Type",f"{ctype}; charset=utf-8")
        self.send_header("Cache-Control","no-store"); self.send_header("Content-Length",str(len(body)))
        self.end_headers(); self.wfile.write(body)

    def _read_json(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n <= 0: return {}
        try: return json.loads(self.rfile.read(n))
        except Exception: return {}

    def parse_request(self) -> bool:
        if not super().parse_request():
            return False
        if self._origin_allowed():
            return True
        self._text("forbidden: local same-origin requests only", status=403)
        return False

    def _origin_allowed(self) -> bool:
        """Refuse foreign Host headers (DNS rebinding) and foreign Origins (cross-site requests)."""
        host = (self.headers.get("Host") or "").lower()
        if host.rsplit(":", 1)[0] not in _LOCAL_HOSTS:
            return False
        origin = self.headers.get("Origin")
        return origin is None or urllib.parse.urlsplit(origin.lower()).netloc == host

    def do_GET(self):
        try:
            raw = self.path
            path = raw.split("?",1)[0]
            qs = urllib.parse.parse_qs(raw.split("?",1)[1]) if "?" in raw else {}
            if path in ("/", "/index.html"): return self._text(INDEX_HTML, "text/html")
            if path == "/healthz": return self._text("ok")
            if path == "/api/snapshot": return self._json(build_snapshot())
            if path == "/api/stream": return self._sse()
            if path == "/view": return self._text(render_file_view((qs.get("path") or [""])[0]), "text/html")
            if path.startswith("/api/messages/"):
                sid = path.rsplit("/",1)[-1]
                return self._json(_oc("GET", f"/session/{sid}/message?limit=50") or [])
            if path == "/api/admin": return self._json(self._admin_snapshot((qs.get("raw") or [""])[0] == "1"))
            if path == "/api/providers": return self._api_providers()
            if path == "/api/models": return self._json(self._api_models())
            if path == "/api/logs": return self._json({"lines": list(_LOG_BUF)})
            if path == "/api/server/status":
                return self._json({"alive": _is_opencode_alive(), "processes": _opencode_processes()})
            if path.startswith("/api/wait/"):
                sid = path.rsplit("/", 1)[-1]
                timeout = int((qs.get("timeout") or ["300"])[0])
                return self._wait_completion(sid, min(timeout, 600))
            return self._text("not found", status=404)
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError): return
        except Exception:
            tb = traceback.format_exc(); _log("GET error: "+tb)
            try: self._text(tb, status=500)
            except Exception: pass

    def do_POST(self):
        try:
            path = self.path.split("?",1)[0]
            if path == "/api/fire":
                body = self._read_json()
                prompt = body.get("prompt") or ""
                provider = body.get("provider") or "opencode"
                model = body.get("model") or "big-pickle"
                title = body.get("title") or f"{model.split('/')[-1]}: untitled"
                listed = self._api_models().get("models") or []
                if not any(m["provider"] == provider and m["id"] == model for m in listed):
                    return self._json({"error": f"model {provider}/{model} is not offered by a connected provider"}, status=400)
                sess = _oc("POST", "/session", {"title": title})
                if not isinstance(sess, dict) or "id" not in sess:
                    return self._json({"error":"session create failed","detail":sess}, status=500)
                sid = sess["id"]
                res = _oc("POST", f"/session/{sid}/prompt_async", {
                    "parts":[{"type":"text","text":prompt}],
                    "model":{"providerID":provider,"modelID":model}, "agent":"build",
                }, root=True)
                _log(f"fire sid={sid} title={title!r} model={provider}/{model}")
                if _oc_error(res):
                    return self._json({"error":f"prompt not confirmed: {_oc_error(res)}", "sessionID":sid, "detail":res}, status=502)
                return self._json({"ok":True, "sessionID":sid})
            if path.startswith("/api/abort/"):
                sid = path.rsplit("/",1)[-1]
                _log(f"abort sid={sid}")
                # No request body: OpenCode resets the connection when this route leaves a body unread.
                abort_res = _oc("POST", f"/session/{sid}/abort", root=True)
                notes = []
                if _oc_error(abort_res):
                    notes.append(_oc_error(abort_res))
                    _log(f"abort sid={sid} failed: {notes[0]}")
                # Nudge an immediate SSE snapshot so the UI reflects the aborted session.
                try: _broadcast("snapshot", build_snapshot())
                except Exception: pass
                return self._json({"ok":True, "abort": abort_res, "notes": notes})
            if path.startswith("/api/reply/"):
                sid = path.rsplit("/",1)[-1]
                body = self._read_json() or {}
                prompt = body.get("prompt", "")
                provider = body.get("provider")
                model = body.get("model")
                msg_body: dict = {
                    "parts":[{"type":"text","text":prompt}],
                    "agent":"build",
                }
                if provider or model:
                    msg_body["model"] = {"providerID": provider or "opencode", "modelID": model or "big-pickle"}
                res = _oc("POST", f"/session/{sid}/prompt_async", msg_body, root=True)
                _log(f"reply sid={sid}" + (f" model={provider}/{model}" if model else ""))
                if _oc_error(res):
                    return self._json({"error":f"prompt not confirmed: {_oc_error(res)}", "detail":res}, status=502)
                return self._json({"ok":True})
            if path.startswith("/api/permission/"):
                # Shape: POST /api/permission/<sid>/<pid> body {"reply": "once|always|reject"}
                # Backend route is /permission/<pid>/reply with the same body.
                # (The /session/<sid>/permissions/<pid> variant uses {"response": "..."}
                # and we don't need the session id - reply route is simpler.)
                parts = path.split("/")
                pid = parts[-1]
                body = self._read_json()
                reply = body.get("reply", "once")
                return self._json({"ok":True, "result": _oc("POST", f"/permission/{pid}/reply{_REQUEST_DIRS.get(pid, '')}", {"reply":reply})})
            if path.startswith("/api/question/"):
                # Shape: POST /api/question/<requestId>/reply  body {"answers": [["label"], ...]}
                # or {"answer": "label"} for single-question convenience.
                # Forwards to opencode /question/<requestId>/reply.
                parts = path.split("/")
                qid = parts[-2] if parts[-1] == "reply" else parts[-1]
                body = self._read_json() or {}
                if "answers" in body:
                    answers = body["answers"]
                else:
                    ans = body.get("answer", "")
                    answers = [[ans]] if ans else [[""]]
                _log(f"question sid-reply qid={qid} answers={answers!r}")
                result = _oc("POST", f"/question/{qid}/reply{_REQUEST_DIRS.get(qid, '')}", {"answers": answers})
                err = _oc_error(result)
                if err:
                    return self._json({"error": err, "detail": result}, status=502)
                return self._json({"ok": True, "result": result})
            if path == "/api/admin":
                body = self._read_json()
                for k in ("opencode_url","filter_prefix","cwd_filter"):
                    if k in body:
                        CONFIG[k] = (body[k] or "").rstrip("/") if k=="opencode_url" else (body[k] or "")
                return self._json({"ok":True, "config": CONFIG})
            if path == "/api/env":
                body = self._read_json()
                key = body.get("key"); value = body.get("value")
                if not key: return self._json({"error":"missing key"}, status=400)
                _log(f"env set {key}=***")
                return self._json(opencode_env_set(key, value))
            if path == "/api/server/stop":   return self._json(opencode_stop())
            if path == "/api/server/start":  return self._json(opencode_start())
            if path == "/api/server/restart":return self._json(opencode_restart())
            if path == "/api/invoke":
                b = self._read_json()
                method = (b.get("method") or "GET").upper()
                pth = b.get("path") or "/"
                body = b.get("body")
                status, res = _oc_status(method, pth, body)
                return self._json({"ok":True, "url": CONFIG["opencode_url"].rstrip("/") + pth, "body": res, "status": status})
            return self._text("not found", status=404)
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError): return
        except Exception:
            tb = traceback.format_exc(); _log("POST error: "+tb)
            try: self._text(tb, status=500)
            except Exception: pass

    def do_DELETE(self):
        return self._text("not found", status=404)

    def _admin_snapshot(self, raw: bool = False):
        env = opencode_env_mask()
        uptime = opencode_server_uptime()
        health = api_key_health()
        snap = {
            "opencode_url": CONFIG["opencode_url"],
            "filter_prefix": CONFIG["filter_prefix"],
            "cwd_filter": CONFIG["cwd_filter"],
            "idle_cutoff_ms": CONFIG["idle_cutoff_ms"],
            "env": env,
            "alive": _is_opencode_alive(),
            "processes": _opencode_processes(),
            "uptime": uptime,
            "api_health": health,
            "discovered_keys": discover_api_keys(),
        }
        if raw:
            snap["env_raw"] = {k: os.environ.get(k) for k in env}
        return snap

    def _api_providers(self):
        """Lazy endpoint for connected providers; the full catalog is too large for the browser."""
        prov = _oc("GET", "/provider", timeout=10.0)
        if not isinstance(prov, dict) or "all" not in prov:
            return self._json({"providers": None})
        connected = set(prov.get("connected") or [])
        return self._json({"providers": {
            "all": [p for p in prov["all"] if p.get("id") in connected],
            "connected": sorted(connected),
            "default": prov.get("default"),
        }})

    def _api_models(self):
        """Return models from the providers OpenCode reports as connected.
        Cached 30 s like api_key_health to avoid re-fetching the ~8 MB
        provider catalog on every Fire modal open."""
        now = time.time()
        if _models_cache["value"] is not None and now - _models_cache["ts"] < 30.0:
            return _models_cache["value"]
        prov = _oc("GET", "/provider", timeout=10.0)
        if not isinstance(prov, dict):
            return {"models": [], "error": "provider endpoint unreachable"}
        models = []
        configured = []
        connected = set(prov.get("connected") or [])
        for p in prov.get("all") or []:
            pid = p.get("id") or ""
            if pid not in connected:
                continue
            configured.append(pid)
            for mid, minfo in (p.get("models") or {}).items():
                models.append({
                    "provider": pid,
                    "id": mid,
                    "name": (minfo or {}).get("name") or mid,
                })
        models.sort(key=lambda m: (m["provider"], m["name"].lower()))
        result = {"models": models, "providers": sorted(configured)}
        _models_cache["ts"] = now
        _models_cache["value"] = result
        return result

    def _wait_completion(self, sid: str, timeout_sec: int):
        """Long-poll until a session reaches a terminal state or timeout."""
        _TERMINAL = ("completed", "idle", "failed", "error")
        deadline = time.time() + timeout_sec
        last_status = None
        while time.time() < deadline:
            sess = _oc("GET", f"/session/{sid}", timeout=3.0)
            if not isinstance(sess, dict) or "id" not in sess:
                return self._json({"sid": sid, "status": "not_found", "detail": sess}, status=404)
            msgs = _oc("GET", f"/session/{sid}/message?limit=1", timeout=2.0)
            last_msg = None
            if isinstance(msgs, list) and msgs:
                last_msg = msgs[-1]
            perms = _oc("GET", "/permission", timeout=2.0)
            has_perm = any(p.get("sessionID") == sid for p in (perms if isinstance(perms, list) else []))
            qs = _oc("GET", "/question", timeout=2.0)
            has_q = any(q.get("sessionID") == sid for q in (qs if isinstance(qs, list) else []))
            now_ms = int(time.time() * 1000)
            derived = classify_session(sess, last_msg, has_perm, now_ms, has_q)
            last_status = derived["status"]
            if last_status in _TERMINAL or last_status == "input_required":
                title = sess.get("title") or ""
                tokens = sess.get("tokens") or {}
                return self._json({
                    "sid": sid, "title": title,
                    "status": last_status, "stage": derived["stage"],
                    "tokens": tokens,
                    "has_pending_permissions": has_perm,
                    "has_pending_questions": has_q,
                })
            time.sleep(POLL_INTERVAL_SEC)
        return self._json({
            "sid": sid, "status": "timeout",
            "last_seen_status": last_status,
            "timeout_sec": timeout_sec,
        })

    def _sse(self):
        self.send_response(200); self.send_header("Content-Type","text/event-stream")
        self.send_header("Cache-Control","no-store"); self.send_header("Connection","keep-alive")
        self.send_header("X-Accel-Buffering","no"); self.end_headers()
        q: queue.Queue = queue.Queue(maxsize=50)
        with _subs_lock: _subscribers.append(q)
        try:
            self.wfile.write(f"event: snapshot\ndata: {json.dumps(build_snapshot())}\n\n".encode()); self.wfile.flush()
        except Exception: pass
        try:
            while True:
                try:
                    chunk = q.get(timeout=30)
                    self.wfile.write(chunk.encode()); self.wfile.flush()
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n"); self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError): pass
        finally:
            with _subs_lock:
                try: _subscribers.remove(q)
                except ValueError: pass

    def log_message(self, fmt, *args): pass


def _env_port() -> int | None:
    """Honor the PORT env var Claude Code sets when launching via launch.json
    so preview_start can route this process to whatever free port it picked."""
    for key in ("PORT", "OPENCODE_PANEL_PORT"):
        v = os.environ.get(key)
        if v and v.isdigit():
            return int(v)
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=None,
                    help=f"Port to listen on. Falls back to $PORT env, then {DEFAULT_PORT}.")
    ap.add_argument("--opencode", default=DEFAULT_OPENCODE)
    ap.add_argument("--filter", default="")
    ap.add_argument("--cwd-filter", default="")
    ap.add_argument("--idle-cutoff-sec", type=int, default=IDLE_CUTOFF_MS//1000)
    ap.add_argument("--view-root", action="append", default=[],
                    help="Directory the file viewer may read; repeatable. OPENCODE_PANEL_VIEW_ROOTS "
                         "(os.pathsep-separated) adds more. The working directory is always allowed.")
    args = ap.parse_args()
    env_roots = [r for r in os.environ.get("OPENCODE_PANEL_VIEW_ROOTS", "").split(os.pathsep) if r]
    _VIEWER_ROOTS[:] = [os.path.abspath(r) for r in [".", *env_roots, *args.view_root]]
    # Port precedence: --port flag > PORT env > OPENCODE_PANEL_PORT env > default.
    chosen_port = args.port if args.port is not None else (_env_port() or DEFAULT_PORT)
    args.port = chosen_port
    CONFIG["opencode_url"] = args.opencode.rstrip("/")
    CONFIG["filter_prefix"] = args.filter
    CONFIG["cwd_filter"] = args.cwd_filter
    CONFIG["idle_cutoff_ms"] = args.idle_cutoff_sec * 1000
    threading.Thread(target=poller_loop, daemon=True).start()

    def _prewarm():
        # Prime the slow caches so the first browser hit doesn't block on them.
        try: _opencode_processes()
        except Exception: pass
        try: opencode_server_uptime()
        except Exception: pass
        try: api_key_health()
        except Exception: pass
    threading.Thread(target=_prewarm, daemon=True).start()
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    msg = f"opencode-panel listening on http://127.0.0.1:{args.port}"
    print(msg, flush=True); _log(msg)
    print(f"  opencode: {CONFIG['opencode_url']}", flush=True)
    print(f"  filter: {CONFIG['filter_prefix']!r} cwd_filter: {CONFIG['cwd_filter']!r}", flush=True)
    try: srv.serve_forever()
    except KeyboardInterrupt: pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
