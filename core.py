"""lm_route_proxy core: HTTP server, upstream I/O, orchestration."""

from __future__ import annotations

import asyncio
import contextlib
import copy
import json
import logging
import os
import re
import time
from typing import Any, Optional

from aiohttp import ClientError, ClientSession, ClientTimeout, TCPConnector, web

import routing

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
SETTINGS_PATH = os.path.join(BASE_DIR, "settings.json")
LOG_DIR = os.path.join(BASE_DIR, "_temp_")
LOG_PATH = os.path.join(LOG_DIR, "temp.log")

COMPAT_PATHS = ("chat/completions", "responses")

HOP_HEADERS = frozenset({
    "host", "content-length", "connection", "keep-alive",
    "proxy-authenticate", "proxy-authorization", "te", "trailer",
    "transfer-encoding", "upgrade", "authorization", "accept-encoding",
})

SAFE_RESPONSE_HEADERS = frozenset({"x-request-id", "x-generation-id", "cache-control"})
SAFE_RESPONSE_PREFIXES = ("x-openrouter-cache",)

ENV_KEYS = (
    "UPSTREAM_BASE_URL", "UPSTREAM_API_KEY", "BIND_HOST", "PORT",
    "LOG_LEVEL", "PROVIDERS_POLICY_URL",
)

DEFAULT_SETTINGS: dict[str, Any] = {
    "tps_prio": 0.40,
    "latency_prio": 0.30,
    "price_prio": 0.20,
    "latency_percentiles": ["p50", "p75", "p90"],
    "tps_percentiles": ["p50", "p75", "p90"],
    "price_output_token_estimate": 512,
    "cache_hit_neutral": 0.5,
    "stick_activate_overlap_p": 0.40,
    "stick_power": 1.5,
    "stick_max_boost": 0.4,
    "history_max": 80,
    "route_history_ttl_sec": 300,
    "provider_metadata_ttl_sec": 300,
    "privacy_policy_ttl_sec": 21600,
    "privacy_fail_closed": False,
    "providers_allow_training": False,
    "providers_allow_training_openrouter": False,
    "providers_allow_retains_prompts": True,
    "providers_allow_can_publish": False,
    "provider_require_capabilities": [],
    "provider_only_max": 10,
    "upstream_timeout_sec": 300,
    "fill_defaults_if_absent": {},
    "force_body_overrides": {},
    "prompt_cache": {"enabled": False, "ttl_sec": 300},
    "per_model_overrides": {},
}

log = logging.getLogger("lm_route_proxy")


def load_environment_variables(path: str = ENV_PATH) -> dict[str, str]:
    vals: dict[str, str] = {}
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                vals[k.strip()] = v.strip().strip('"').strip("'")
    for k in ENV_KEYS:
        if os.environ.get(k):
            vals[k] = os.environ[k]
    return vals


def _merge_settings(base: dict, override: dict) -> dict:
    """Recursive dict merge; scalars and lists replace."""
    out = copy.deepcopy(base)
    for k, v in override.items():
        cur = out.get(k)
        if isinstance(cur, dict) and isinstance(v, dict):
            out[k] = _merge_settings(cur, v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def _strip_json_comments(text: str) -> str:
    """Strip JSON comments outside strings."""
    def _replace_json_comment(m: re.Match) -> str:
        s = m.group(0)
        return s if s[0] == '"' else "\n" * s.count("\n")
    return re.sub(r'("(?:\\.|[^"\\])*"?)|//[^\n]*|/\*.*?(?:\*/|$)', _replace_json_comment, text, flags=re.DOTALL)


class SettingsStore:
    """settings.json snapshot with background reload."""

    def __init__(self, path: str = SETTINGS_PATH):
        self._path = path
        self._snap: dict[str, Any] = copy.deepcopy(DEFAULT_SETTINGS)
        self._mtime = 0.0
        self._on_reload = None
        self._warned_missing = False
        try:
            self._reload_settings_from_disk()
        except Exception as e:
            log.error("settings: keeping defaults (%s)", e)

    def current_settings_snapshot(self) -> dict[str, Any]:
        return self._snap

    def register_reload_callback(self, cb) -> None:
        self._on_reload = cb

    def _reload_settings_from_disk(self) -> None:
        st = os.stat(self._path)
        with open(self._path, "r", encoding="utf-8") as f:
            data = json.loads(_strip_json_comments(f.read()))
        if not isinstance(data, dict):
            raise ValueError("settings root must be a JSON object")
        merged = _merge_settings(DEFAULT_SETTINGS, data)
        self._snap = merged
        self._mtime = st.st_mtime
        self._warned_missing = False
        if self._on_reload is not None:
            try:
                self._on_reload(merged)
            except Exception as e:
                log.error("settings reload callback failed: %s", e)

    async def watch_settings_file(self) -> None:
        while True:
            await asyncio.sleep(2.0)
            try:
                st = os.stat(self._path)
            except FileNotFoundError:
                if not self._warned_missing:
                    log.warning("settings: file missing; keeping last valid config")
                    self._warned_missing = True
                continue
            try:
                if st.st_mtime != self._mtime:
                    self._reload_settings_from_disk()
                    log.info("settings reloaded")
            except Exception as e:
                log.error("settings reload failed; keeping last valid config: %s", e)


def _parse_optional_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _iterate_payload_entries(payload: Any) -> list:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        d = payload.get("data", payload)
        if isinstance(d, list):
            return d
        if isinstance(d, dict):
            return list(d.values())
    return []


class MetadataStore:
    """Cached upstream metadata with TTL and stale fallback."""

    def __init__(self, session: ClientSession, base_url: str, settings: SettingsStore,
                 api_key: str, policy_url: Optional[str] = None):
        self._session = session
        self._base = base_url.rstrip("/")
        self._settings = settings
        self._api_key = api_key
        self._policy_url = policy_url or (
            re.sub(r"(?i)/api/v1/?$", "", self._base).rstrip("/") + "/api/frontend/v1/all-providers"
        )
        self._endpoints: dict[str, tuple[float, list]] = {}
        self._single: dict[str, tuple[float, dict]] = {}
        self._inflight: dict[str, asyncio.Future] = {}

    def _build_authorization_headers(self) -> dict:
        return {"Authorization": f"Bearer {self._api_key}"} if self._api_key else {}

    def is_policy_loaded(self) -> bool:
        e = self._single.get("policy")
        return bool(e and e[1])

    def count_cached_models(self) -> int:
        return len(self._endpoints)

    async def _fetch_json_response(self, method: str, url: str, **kw) -> Any:
        kw.setdefault("headers", self._build_authorization_headers())
        kw.setdefault("timeout", ClientTimeout(total=30))
        async with self._session.request(method, url, **kw) as resp:
            resp.raise_for_status()
            return await resp.json(content_type=None)

    async def _refresh_cached_value(self, key: str, cache: dict, fetch):
        """Serialize identical concurrent refreshes."""
        fut = self._inflight.get(key)
        if fut is not None:
            return await asyncio.shield(fut)
        fut = asyncio.get_running_loop().create_future()
        self._inflight[key] = fut
        try:
            val = await fetch()
            cache[key] = (time.time(), val)
            fut.set_result(val)
            return val
        except BaseException as e:
            fut.set_exception(e)
            with contextlib.suppress(Exception):
                fut.exception()
            raise
        finally:
            self._inflight.pop(key, None)

    async def _get_cached_or_fetched(self, cache: dict, key: str, ttl: float, fetch, default: Any):
        cached = cache.get(key)
        if cached and time.time() - cached[0] < ttl:
            return cached[1]
        try:
            return await self._refresh_cached_value(key, cache, fetch)
        except Exception as e:
            if cached:
                log.warning("metadata degraded: %s served stale (%s: %s)", key, type(e).__name__, e)
                return cached[1]
            log.error("metadata failure: %s: %s", key, e)
            return default

    async def get_model_endpoints(self, model: str) -> Optional[list[dict]]:
        s = self._settings.current_settings_snapshot()
        ttl = float(s.get("provider_metadata_ttl_sec", 300))
        return await self._get_cached_or_fetched(
            self._endpoints, f"endpoints:{model}", ttl,
            lambda: self._fetch_model_endpoints(model), default=None,
        )

    async def get_provider_policy(self) -> dict:
        s = self._settings.current_settings_snapshot()
        ttl = float(s.get("privacy_policy_ttl_sec", 21600))
        return await self._get_cached_or_fetched(
            self._single, "policy", ttl, self._fetch_provider_policy, default={},
        )

    async def _fetch_model_endpoints(self, model: str) -> list[dict]:
        payload = await self._fetch_json_response("GET", f"{self._base}/models/{model}/endpoints")
        data = payload.get("data", payload) if isinstance(payload, dict) else payload
        entries = data.get("endpoints", []) if isinstance(data, dict) else data
        if not isinstance(entries, list):
            entries = []
        out = []
        for e in entries:
            if not isinstance(e, dict):
                continue
            slug = e.get("provider_name")
            if not slug:
                name = e.get("name") or ""
                slug = name.rsplit("/", 1)[-1] if "/" in name else None
            if not slug:
                continue
            pricing = e.get("pricing") or {}
            cache = e.get("cache_pricing") or {}
            caps = e.get("supported_parameters")
            if not isinstance(caps, list):
                caps = e.get("capabilities")
            out.append({
                "slug": slug,
                "prompt_price": _parse_optional_float(pricing.get("prompt")),
                "completion_price": _parse_optional_float(pricing.get("completion")),
                "cache_read_price": _parse_optional_float(cache.get("read")),
                "cache_write_price": _parse_optional_float(cache.get("write")),
                "latency": e.get("latency_last_30m") or {},
                "throughput": e.get("throughput_last_30m") or {},
                "capabilities": caps if isinstance(caps, list) else None,
            })
        return out

    async def _fetch_provider_policy(self) -> dict:
        payload = await self._fetch_json_response("GET", self._policy_url)
        out = {}
        for r in _iterate_payload_entries(payload):
            if not isinstance(r, dict):
                continue
            slug = r.get("slug") or r.get("id")
            if not slug or not isinstance(slug, str):
                continue
            dp = r.get("dataPolicy") or r.get("data_policy") or {}
            if not isinstance(dp, dict):
                dp = {}
            out[slug.lower()] = {
                "training": dp.get("training"),
                "training_openrouter": dp.get("trainingOpenRouter"),
                "retains_prompts": dp.get("retainsPrompts"),
                "can_publish": dp.get("canPublish"),
            }
        return out

    async def refresh_policy_periodically(self) -> None:
        while True:
            delay = 1.0
            for attempt in range(1, 5):
                try:
                    await self._refresh_cached_value("policy", self._single, self._fetch_provider_policy)
                    break
                except Exception as e:
                    log.warning(
                        "policy refresh failed attempt=%s/4 reason=%s: %s",
                        attempt, type(e).__name__, e,
                    )
                    if attempt < 4:
                        await asyncio.sleep(delay)
                        delay *= 2
            ttl = max(60.0, float(self._settings.current_settings_snapshot().get("privacy_policy_ttl_sec", 21600)))
            await asyncio.sleep(ttl)


def _build_error_response(status: int, code: str, message: str) -> web.Response:
    return web.json_response(
        {"error": {"message": message, "type": "proxy_error", "code": code}},
        status=status,
    )


def _select_safe_response_headers(headers) -> dict:
    out = {}
    for k, v in headers.items():
        lk = k.lower()
        if lk in SAFE_RESPONSE_HEADERS or lk.startswith(SAFE_RESPONSE_PREFIXES):
            out[k] = v
    return out


def _build_upstream_timeout(settings: dict, stream: bool) -> ClientTimeout:
    secs = float(settings.get("upstream_timeout_sec", 300))
    if stream:
        return ClientTimeout(total=None, sock_connect=30, sock_read=secs)
    return ClientTimeout(total=secs, sock_connect=30)


class _ClientGone(Exception):
    pass


async def _wait_for_client_disconnect(request: web.Request) -> None:
    t = getattr(request, "transport", None)
    if t is None:
        proto = getattr(request, "protocol", None)
        t = getattr(proto, "transport", None) if proto is not None else None
    if t is None:
        await asyncio.Event().wait()
        return
    while not t.is_closing():
        await asyncio.sleep(0.5)


async def _race_upstream_against_client_disconnect(request: web.Request, coro):
    """Cancel upstream when client disconnects."""
    task = asyncio.ensure_future(coro)
    watcher = asyncio.ensure_future(_wait_for_client_disconnect(request))
    try:
        done, _ = await asyncio.wait({task, watcher}, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            return task.result()
        task.cancel()
        with contextlib.suppress(BaseException):
            await task
        raise _ClientGone()
    except asyncio.CancelledError:
        task.cancel()
        with contextlib.suppress(BaseException):
            await task
        raise
    finally:
        if not watcher.done():
            watcher.cancel()
            with contextlib.suppress(BaseException):
                await watcher


_SSE_PROBE_CAP = 64 * 1024


def _extract_served_provider_slug(payload: Any) -> Optional[str]:
    """Provider slug that served the response."""
    if not isinstance(payload, dict):
        return None
    top = payload.get("provider")
    if isinstance(top, str) and top:
        return top
    meta = payload.get("openrouter_metadata")
    available = None
    if isinstance(meta, dict):
        endpoints = meta.get("endpoints")
        if isinstance(endpoints, dict):
            available = endpoints.get("available")
    for e in available or []:
        if isinstance(e, dict) and e.get("selected") is True:
            slug = e.get("provider")
            return slug if isinstance(slug, str) and slug else None
    return None


async def _relay_stream_to_client(request: web.Request, upstream, resp: web.StreamResponse,
                        log_ctx: dict) -> tuple[Optional[str], bool]:
    served: Optional[str] = None
    done = False
    tail = b""
    probe = b""
    try:
        async for chunk in upstream.content.iter_any():
            await resp.write(chunk)
            if served is None:
                probe = (probe + chunk)[:_SSE_PROBE_CAP]
                while served is None and b"\n" in probe:
                    line, probe = probe.split(b"\n", 1)
                    line = line.strip()
                    if line.startswith(b"data:"):
                        with contextlib.suppress(ValueError):
                            served = _extract_served_provider_slug(json.loads(line[5:].strip()))
            if not done:
                tail = (tail + chunk)[-64:]
                if b"[DONE]" in tail:
                    done = True
    except asyncio.TimeoutError:
        log_ctx["result"] = "upstream_timeout"
        with contextlib.suppress(Exception):
            await resp.write(
                b'data: {"error":{"message":"upstream stream stalled past the configured timeout",'
                b'"type":"proxy_error","code":"upstream_timeout"}}\n\n'
            )
    except (ConnectionResetError, ConnectionError, asyncio.CancelledError):
        log_ctx["result"] = "client_disconnect"
    with contextlib.suppress(Exception):
        await resp.write_eof()
    return served, done


async def _record_served_provider_history(app: web.Application, ctx: dict, log_ctx: dict,
                                  served: Optional[str], stream: bool) -> None:
    if served:
        await app["history"].record_successful_route(model=ctx["model"], units=ctx["units"],
                                    mode=ctx["mode"], provider=served)
        log_ctx["served"] = served
    else:
        log_ctx["served"] = None
        if log_ctx.get("only"):
            log_ctx["served_requested"] = log_ctx["only"]
        else:
            log.warning("route history skipped: no served provider in %s "
                        "(cache hit or scrubbed body)", "stream" if stream else "body")


async def _forward_upstream_response(request: web.Request, upstream, ctx: dict,
                   log_ctx: dict, t0: float, stream: bool) -> web.StreamResponse:
    if stream:
        resp = web.StreamResponse(status=upstream.status)
        resp.content_type = upstream.content_type or "text/event-stream"
        for k, v in _select_safe_response_headers(upstream.headers).items():
            resp.headers[k] = v
        resp.headers["Cache-Control"] = "no-cache"
        resp.headers["X-Accel-Buffering"] = "no"
        try:
            await resp.prepare(request)
        except (ConnectionResetError, ConnectionError, asyncio.CancelledError):
            _log_client_disconnect_outcome(log_ctx, upstream.status, t0)
            return resp
        served, complete = await _relay_stream_to_client(request, upstream, resp, log_ctx)
        response = resp
    else:
        try:
            data = await upstream.read()
        except asyncio.CancelledError:
            _log_client_disconnect_outcome(log_ctx, upstream.status, t0)
            return web.Response(status=499)
        served = None
        if 200 <= upstream.status < 300:
            with contextlib.suppress(ValueError):
                served = _extract_served_provider_slug(json.loads(data))
        response = web.Response(
            body=data,
            status=upstream.status,
            content_type=upstream.content_type or "application/json",
            headers=_select_safe_response_headers(upstream.headers),
        )
        complete = True

    if 200 <= upstream.status < 300 and complete:
        log_ctx["result"] = "stream_done" if stream else "ok"
        await _record_served_provider_history(request.app, ctx, log_ctx, served, stream)
    elif "result" not in log_ctx:
        log_ctx["result"] = "stream_incomplete" if stream else "upstream_error"
    _log_route_outcome(log_ctx, upstream.status, t0)
    return response


def _log_client_disconnect_outcome(log_ctx: dict, upstream_status: Optional[int], t0: float) -> None:
    log_ctx["result"] = "client_disconnect"
    _log_route_outcome(log_ctx, upstream_status, t0)


def _log_route_outcome(ctx: dict, status: Optional[int], t0: float) -> None:
    parts = [f"{k}={v}" for k, v in ctx.items() if v is not None]
    parts.append(f"upstream_status={status}")
    parts.append(f"ms={int((time.monotonic() - t0) * 1000)}")
    log.info("route %s", " ".join(str(p) for p in parts))


async def _handle_health_request(request: web.Request) -> web.Response:
    app = request.app
    meta: MetadataStore = app["metadata"]
    return web.json_response({
        "status": "ok",
        "upstream_base": app["env"].get("UPSTREAM_BASE_URL"),
        "upstream_key_configured": bool(app["env"].get("UPSTREAM_API_KEY")),
        "policy_loaded": meta.is_policy_loaded(),
        "models_cached": meta.count_cached_models(),
    })


def _build_upstream_request(body: dict, settings: dict, request: web.Request,
                            env: dict, only: list) -> tuple[dict, dict, str]:
    out_body = dict(body)
    client_provider = out_body.get("provider")
    merged_provider = dict(client_provider) if isinstance(client_provider, dict) else {}
    merged_provider["only"] = list(only)
    merged_provider["allow_fallbacks"] = True
    out_body["provider"] = merged_provider
    for k, v in (settings.get("force_body_overrides") or {}).items():
        out_body[k] = v
    for k, v in (settings.get("fill_defaults_if_absent") or {}).items():
        out_body.setdefault(k, v)

    headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_HEADERS}
    headers["Authorization"] = f"Bearer {env.get('UPSTREAM_API_KEY', '')}"
    headers["X-OpenRouter-Metadata"] = "enabled"
    pc = settings.get("prompt_cache") or {}
    if pc.get("enabled"):
        headers["X-OpenRouter-Cache"] = "true"
        if pc.get("ttl_sec"):
            headers["X-OpenRouter-Cache-TTL"] = str(int(pc["ttl_sec"]))

    op_path = request.path[3:] if request.path.startswith("/v1/") else request.path
    url = env.get("UPSTREAM_BASE_URL", "https://openrouter.ai/api/v1").rstrip("/") + op_path
    return out_body, headers, url


async def _handle_completion_request(request: web.Request) -> web.StreamResponse:
    app = request.app
    t0 = time.monotonic()
    log_ctx: dict[str, Any] = {"op": request.path, "model": None, "stream": None}
    try:
        raw = await request.read()
    except web.HTTPRequestEntityTooLarge:
        return _build_error_response(413, "invalid_request_error", "request body exceeds 96 MiB limit")
    except Exception:
        return _build_error_response(400, "invalid_request_error", "failed to read request body")
    try:
        body = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        return _build_error_response(400, "invalid_request_error", "request body must be valid JSON")
    if not isinstance(body, dict):
        return _build_error_response(400, "invalid_request_error", "request body must be a JSON object")

    model = body.get("model")
    if not isinstance(model, str) or not model:
        return _build_error_response(400, "invalid_request_error", "missing required field: model")
    model = re.sub(r"@preset/.*$", "", model)
    if not model:
        return _build_error_response(400, "invalid_request_error", "missing required field: model")

    env = app["env"]
    api_key = env.get("UPSTREAM_API_KEY", "")
    if not api_key:
        return _build_error_response(500, "missing_credentials",
                    "proxy has no upstream API key configured (set UPSTREAM_API_KEY in .env)")

    settings = dict(app["settings"].current_settings_snapshot())
    per_model = (settings.get("per_model_overrides") or {}).get(model)
    if isinstance(per_model, dict):
        settings.update(per_model)

    stream = body.get("stream") is True
    log_ctx.update(model=model, stream=stream)

    meta: MetadataStore = app["metadata"]
    providers = await meta.get_model_endpoints(model)
    if providers is None:
        return _build_error_response(503, "metadata_failure",
                    f"provider metadata unavailable for model '{model}'; retry shortly")
    policy = await meta.get_provider_policy()

    result = await routing.decide_route(
        model=model, body=body, providers=providers, policy=policy,
        settings=settings, history=app["history"],
    )
    only = result["provider_only"]
    ctx = result["ctx"]
    dbg = result["debug"]
    log_ctx.update(candidates=dbg.get("candidates"), eligible=dbg.get("eligible"),
                   drops=dbg.get("drops") or None, only=only,
                   overlap=dbg.get("overlap"), stick_boost=dbg.get("stick_boost") or None)
    if not only:
        return _build_error_response(404, "no_eligible_provider",
                    f"no eligible provider for model '{model}' under current constraints; "
                    f"drop_reasons={dbg.get('drops')}")

    out_body, headers, url = _build_upstream_request(body, settings, request, env, only)
    session: ClientSession = app["session"]
    upstream = None
    try:
        try:
            upstream = await _race_upstream_against_client_disconnect(
                request,
                session.post(url, json=out_body, headers=headers, timeout=_build_upstream_timeout(settings, stream)),
            )
        except asyncio.TimeoutError:
            _log_route_outcome(log_ctx, None, t0)
            return _build_error_response(504, "upstream_timeout", "upstream did not respond within the configured timeout")
        except _ClientGone:
            log_ctx["result"] = "client_disconnect"
            _log_route_outcome(log_ctx, None, t0)
            return _build_error_response(400, "request_cancelled", "client disconnected before upstream responded")
        except ClientError as e:
            _log_route_outcome(log_ctx, None, t0)
            return _build_error_response(502, "upstream_error", f"upstream connection failed: {type(e).__name__}")

        try:
            is_sse = (upstream.content_type or "").startswith("text/event-stream")
            return await _forward_upstream_response(request, upstream, ctx, log_ctx, t0, stream and is_sse)
        except asyncio.TimeoutError:
            log_ctx["result"] = "upstream_timeout"
            _log_route_outcome(log_ctx, upstream.status, t0)
            return _build_error_response(504, "upstream_timeout", "upstream stream stalled past the configured timeout")
    finally:
        if upstream is not None:
            with contextlib.suppress(Exception):
                upstream.close()


def build_app(env: dict, settings: SettingsStore) -> web.Application:
    app = web.Application(client_max_size=96 * 1024 * 1024)
    app["env"] = env
    app["settings"] = settings

    async def _start_application(app: web.Application) -> None:
        s = settings.current_settings_snapshot()
        app["session"] = ClientSession(connector=TCPConnector(limit=200, ttl_dns_cache=300))
        app["history"] = routing.RouteHistory(
            int(s.get("history_max", 10)), float(s.get("route_history_ttl_sec", 300)),
        )
        settings.register_reload_callback(lambda snap: app["history"].configure_history_limits(
            int(snap.get("history_max", 10)), float(snap.get("route_history_ttl_sec", 300)),
        ))
        app["metadata"] = MetadataStore(
            app["session"],
            env.get("UPSTREAM_BASE_URL", "https://openrouter.ai/api/v1"),
            settings,
            env.get("UPSTREAM_API_KEY", ""),
            env.get("PROVIDERS_POLICY_URL") or None,
        )
        app["tasks"] = [
            asyncio.create_task(settings.watch_settings_file(), name="settings-watch"),
            asyncio.create_task(app["metadata"].refresh_policy_periodically(), name="policy-refresh"),
        ]
        log.info("proxy started upstream=%s", env.get("UPSTREAM_BASE_URL"))

    async def _stop_application(app: web.Application) -> None:
        for t in app.get("tasks", []):
            t.cancel()
        await asyncio.gather(*app.get("tasks", []), return_exceptions=True)
        sess = app.get("session")
        if sess is not None:
            await sess.close()
        log.info("proxy stopped")

    app.on_startup.append(_start_application)
    app.on_shutdown.append(_stop_application)

    app.router.add_get("/health", _handle_health_request)
    for path in COMPAT_PATHS:
        app.router.add_post(f"/{path}", _handle_completion_request)
        app.router.add_post(f"/v1/{path}", _handle_completion_request)
    return app


def _configure_logging_handlers(level_name: str) -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(getattr(logging, level_name.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    fh = logging.FileHandler(LOG_PATH)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(sh)


def run_proxy_server() -> None:
    env = load_environment_variables()
    _configure_logging_handlers(env.get("LOG_LEVEL", "INFO"))
    settings = SettingsStore(SETTINGS_PATH)
    app = build_app(env, settings)
    web.run_app(
        app,
        host=env.get("BIND_HOST", "127.0.0.1"),
        port=int(env.get("PORT", "9354")),
        access_log=None,
    )


if __name__ == "__main__":
    run_proxy_server()
