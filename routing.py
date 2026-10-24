"""Routing policy adapter."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Optional

NEUTRAL = 0.5

_PRIVACY_GATES = (
    ("providers_allow_training", "training"),
    ("providers_allow_training_openrouter", "training_openrouter"),
    ("providers_allow_retains_prompts", "retains_prompts"),
    ("providers_allow_can_publish", "can_publish"),
)

_CONTEXT_HEADER_KEYS = (
    "tools",
    "tool_choice",
    "response_format",
    "structured_outputs",
    "modalities",
)

_MODE_KEYS = (
    "reasoning",
    "reasoning_effort",
    "parallel_tool_calls",
    "text",
    "previous_response_id",
)


def _canonical_json_string(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def normalize_request_context(model: str, body: dict) -> dict:
    """Canonical routing-only view of a request."""
    header = {k: body[k] for k in _CONTEXT_HEADER_KEYS if k in body}
    mode = _canonical_json_string({k: body[k] for k in _MODE_KEYS if k in body})
    units: list[str] = [_canonical_json_string(header)]
    msgs = body.get("messages")
    if isinstance(msgs, list):
        for m in msgs:
            units.append(_canonical_json_string(
                {k: m[k] for k in ("role", "name", "tool_call_id", "tool_calls", "content", "reasoning") if k in m}
                if isinstance(m, dict) else m
            ))
    inp = body.get("input")
    if isinstance(inp, list):
        for item in inp:
            units.append(_canonical_json_string(item))
    elif isinstance(inp, str):
        units.append(_canonical_json_string(inp))
    if "prompt" in body:
        units.append(_canonical_json_string(body["prompt"]))
    return {"model": model, "units": units, "size": sum(len(u) for u in units), "mode": mode}


def prefix_overlap(current: list[str], prior: list[str]) -> float:
    """Shared-prefix fraction vs the longer request."""
    current_size = sum(len(u) for u in current)
    prior_size = sum(len(u) for u in prior)
    denom = max(current_size, prior_size) or 1
    matched = 0
    for cu, pv in zip(current, prior):
        if cu == pv:
            matched += len(cu)
            continue
        limit = min(len(cu), len(pv))
        i = 0
        while i < limit and cu[i] == pv[i]:
            i += 1
        matched += i
        break
    return matched / denom


def _is_continuation(prior: list[str], new: list[str]) -> bool:
    """True when prior units prefix new units."""
    n = len(prior)
    return n == 0 or (n <= len(new) and prior[: n - 1] == new[: n - 1]
                      and new[n - 1].startswith(prior[n - 1]))


class RouteHistory:
    """In-memory route history bounded by count and TTL."""

    def __init__(self, max_entries: int, ttl_sec: float):
        self._max = max(1, int(max_entries))
        self._ttl = float(ttl_sec)
        self._entries: list[dict] = []
        self._lock = asyncio.Lock()

    def configure_history_limits(self, max_entries: int, ttl_sec: float) -> None:
        self._max = max(1, int(max_entries))
        self._ttl = float(ttl_sec)

    def _evict_expired_entries(self, now: float) -> None:
        self._entries = [e for e in self._entries if now - e["ts"] <= self._ttl]

    async def record_successful_route(self, *, model: str, units: list[str], mode: str, provider: str) -> None:
        """Record a successful eligible route."""
        now = time.time()
        async with self._lock:
            self._evict_expired_entries(now)
            for e in self._entries:
                if (e["model"] == model and e["provider"] == provider and e["mode"] == mode
                        and _is_continuation(e["units"], units)):
                    e["units"], e["ts"] = units, now
                    return
            self._entries.append({"model": model, "units": units, "mode": mode,
                                  "provider": provider, "ts": now})
            overflow = len(self._entries) - self._max
            if overflow > 0:
                self._entries.sort(key=lambda e: e["ts"])
                del self._entries[:overflow]

    async def find_best_overlap(self, *, model: str, units: list[str], mode: str) -> Optional[tuple[float, str, float]]:
        """Strongest compatible prefix match, else None."""
        now = time.time()
        async with self._lock:
            self._evict_expired_entries(now)
            best: Optional[tuple[float, str, float]] = None
            for e in self._entries:
                if e["model"] != model or e["mode"] != mode:
                    continue
                ov = prefix_overlap(units, e["units"])
                if best is None or ov > best[0] or (ov == best[0] and e["ts"] > best[2]):
                    best = (ov, e["provider"], e["ts"])
            return best


def _find_privacy_drop_reason(pol: Any, settings: dict, fail_closed: bool) -> Optional[str]:
    """First violated privacy gate, else None."""
    for skey, flag in _PRIVACY_GATES:
        if bool(settings.get(skey, True)):
            continue
        val = pol.get(flag) if isinstance(pol, dict) else None
        if val is True or (fail_closed and val is not False):
            return f"privacy:{flag}"
    return None


def filter_candidates(
    providers: list[dict],
    policy: dict,
    settings: dict,
    client_provider: Any,
) -> tuple[list[dict], dict[str, str]]:
    """Apply hard constraints; return eligible and drops."""
    drops: dict[str, str] = {}
    eligible: list[dict] = []
    req_caps = set(settings.get("provider_require_capabilities") or [])
    fail_closed = bool(settings.get("privacy_fail_closed", False))

    cp = client_provider if isinstance(client_provider, dict) else {}
    raw_only = cp.get("only")
    c_only = {s for s in raw_only if isinstance(s, str)} if isinstance(raw_only, list) else set()
    if not c_only:
        c_only = None
    c_ignore = {s for s in (cp.get("ignore") or []) if isinstance(s, str)}

    for p in providers:
        slug = p.get("slug")
        if not slug or not isinstance(slug, str):
            drops[slug or "<unknown>"] = "unusable_identity"
            continue
        if c_only is not None and slug not in c_only:
            drops[slug] = "client_only"
            continue
        if slug in c_ignore:
            drops[slug] = "client_ignored"
            continue
        caps = p.get("capabilities")
        if req_caps and not (isinstance(caps, list) and req_caps.issubset(set(caps))):
            drops[slug] = "missing_capability"
            continue
        pol = policy.get(slug.lower()) if isinstance(policy, dict) else None
        reason = _find_privacy_drop_reason(pol, settings, fail_closed)
        if reason:
            drops[slug] = reason
            continue
        eligible.append(p)
    return eligible, drops


def _average_band_value(dist: Any, band: list[str]) -> Optional[float]:
    if not isinstance(dist, dict):
        return None
    vals = [float(dist[k]) for k in band if isinstance(dist.get(k), (int, float))]
    return sum(vals) / len(vals) if vals else None


def _normalize_metric_scores(values: dict[str, Optional[float]], higher_is_better: bool) -> dict[str, float]:
    """Clamp-normalize one metric to 0..1."""
    present = sorted(v for v in values.values() if v is not None)
    if not present:
        return {}
    n = len(present)
    lo, hi = ((present[int(0.05 * (n - 1))], present[int(0.95 * (n - 1))])
              if n >= 4 else (present[0], present[-1]))
    med = present[n // 2] if n % 2 else (present[n // 2 - 1] + present[n // 2]) / 2.0
    out = {}
    for slug, v in values.items():
        v = med if v is None else v
        if hi <= lo:
            out[slug] = NEUTRAL
        else:
            t = (min(max(v, lo), hi) - lo) / (hi - lo)
            out[slug] = t if higher_is_better else 1.0 - t
    return out


def effective_price(
    provider: dict,
    overlap: float,
    settings: dict,
    input_tokens: float,
) -> Optional[float]:
    """Request-context price estimate; routing signal only."""
    prompt = provider.get("prompt_price")
    completion = provider.get("completion_price")
    if prompt is None and completion is None:
        return None
    prompt = float(prompt or 0.0)
    completion = float(completion or 0.0)
    read = provider.get("cache_read_price")
    write = provider.get("cache_write_price")

    hit = float(settings.get("cache_hit_neutral", NEUTRAL))
    cached_share = max(0.0, min(1.0, overlap * hit))

    if read is not None:
        fresh_unit = max(float(write), prompt) if write is not None else prompt
        input_unit = cached_share * float(read) + (1.0 - cached_share) * fresh_unit
    else:
        input_unit = prompt

    output_tokens = float(settings.get("price_output_token_estimate", 512))
    cost_in = input_unit * input_tokens
    cost_out = completion * output_tokens
    input_weight = 0.5 + 0.5 * overlap
    return input_weight * cost_in + (1.0 - input_weight) * cost_out


def _score_eligible_providers(eligible: list[dict], settings: dict, overlap: float, ctx: dict) -> list[list]:
    """Weighted score per eligible provider."""
    band_lat = settings.get("latency_percentiles") or ["p50", "p75"]
    band_tps = settings.get("tps_percentiles") or ["p50", "p75"]
    w_lat = max(0.0, float(settings.get("latency_prio", 0)))
    w_tps = max(0.0, float(settings.get("tps_prio", 0)))
    w_price = max(0.0, float(settings.get("price_prio", 0)))

    lat = {p["slug"]: _average_band_value(p.get("latency"), band_lat) for p in eligible}
    tps = {p["slug"]: _average_band_value(p.get("throughput"), band_tps) for p in eligible}
    input_tokens = max(1, ctx["size"] // 4)
    prices = {p["slug"]: effective_price(p, overlap, settings, input_tokens) for p in eligible}

    metrics = (
        (w_lat, _normalize_metric_scores(lat, False)),
        (w_tps, _normalize_metric_scores(tps, True)),
        (w_price, _normalize_metric_scores(prices, False)),
    )
    scored: list[list] = []
    for p in eligible:
        parts = [(w, scores[p["slug"]]) for w, scores in metrics if w > 0 and p["slug"] in scores]
        base = sum(w * s for w, s in parts) / sum(w for w, _ in parts) if parts else 0.0
        scored.append([p["slug"], base])
    return scored


def _apply_stickiness_boost(scored: list[list], stick_provider: Optional[str], overlap: float, settings: dict) -> float:
    """Boost previously served provider on prefix overlap."""
    threshold = float(settings.get("stick_activate_overlap_p", 0.40))
    if not (stick_provider and overlap >= threshold and stick_provider in {s for s, _ in scored}):
        return 0.0
    power = float(settings.get("stick_power", 1.5))
    cap = float(settings.get("stick_max_boost", 0.85))
    boost = min(cap, overlap ** power)
    for row in scored:
        if row[0] == stick_provider:
            row[1] += boost
    return boost


async def decide_route(
    *,
    model: str,
    body: dict,
    providers: list[dict],
    policy: dict,
    settings: dict,
    history: Optional[RouteHistory],
) -> dict:
    """Rank eligible providers and return route decision."""
    ctx = normalize_request_context(model, body)
    eligible, drops = filter_candidates(providers, policy or {}, settings, body.get("provider"))

    overlap, stick_provider = 0.0, None
    if history is not None and eligible:
        best = await history.find_best_overlap(model=model, units=ctx["units"], mode=ctx["mode"])
        if best is not None:
            overlap, stick_provider, _ts = best

    scored = _score_eligible_providers(eligible, settings, overlap, ctx)
    stick_boost = _apply_stickiness_boost(scored, stick_provider, overlap, settings)
    scored.sort(key=lambda r: (-r[1], r[0]))
    limit = max(1, int(settings.get("provider_only_max", 10)))
    only = [s for s, _ in scored][:limit]

    debug = {
        "candidates": len(providers),
        "eligible": len(eligible),
        "drops": drops,
        "overlap": round(overlap, 4),
        "stick_boost": round(stick_boost, 4),
    }
    return {"provider_only": only, "ctx": ctx, "debug": debug}
