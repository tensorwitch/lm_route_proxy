# lm_route_proxy — manual

Application reference: quick start, configuration, API, errors, observability. Architecture and routing rationale: `README.md`.

## Quick start

Point your client's baseURL at `http://127.0.0.1:9354` instead of `https://openrouter.ai/api/v1`.

```sh
cp .env.example .env          # then set UPSTREAM_API_KEY
python3 core.py
```

Docker:

```sh
docker compose -f docker/docker-compose.yml up --build
docker compose -f docker/docker-compose.yml up -d             # detached
HOST_PORT=8080 docker compose -f docker/docker-compose.yml up # override host port
docker compose -f docker/docker-compose.yml logs -f
docker compose -f docker/docker-compose.yml restart           # apply settings after edit
docker compose -f docker/docker-compose.yml down
```

## Configuration

### `.env` — secrets and deploy

Read once at boot. Values already present in `os.environ` override the file for the keys below.

| key | default | meaning |
|---|---|---|
| `UPSTREAM_BASE_URL` | `https://openrouter.ai/api/v1` | upstream router base |
| `UPSTREAM_API_KEY` | — | OpenRouter bearer key; required |
| `BIND_HOST` | `127.0.0.1` | listen address |
| `PORT` | `9354` | listen port |
| `LOG_LEVEL` | `INFO` | logging level |
| `PROVIDERS_POLICY_URL` | derived | policy-feed override; default strips a trailing `/api/v1` from the base and appends `/api/frontend/v1/all-providers` |

### `settings.json`

Defaults below are the `DEFAULT_SETTINGS` table in `core.py`; the file is deep-merged over them. The file is re-read on mtime change (polled every 2 s), may contain `//` and `/* */` comments, and keeps the last valid configuration on parse error.

| key | default | meaning |
|---|---|---|
| `tps_prio` | `0.4` | throughput weight |
| `latency_prio` | `0.3` | latency weight |
| `price_prio` | `0.2` | price weight |
| `latency_percentiles` | `["p50", "p75", "p90"]` | latency metric buckets |
| `tps_percentiles` | `["p50", "p75", "p90"]` | throughput metric buckets |
| `price_output_token_estimate` | `512` | assumed output tokens for price estimates |
| `cache_hit_neutral` | `0.5` | assumed cache-hit rate for price estimates |
| `stick_activate_overlap_p` | `0.4` | minimum prefix overlap to activate the stickiness boost |
| `stick_power` | `1.5` | exponent on overlap for the boost |
| `stick_max_boost` | `0.4` | boost cap |
| `history_max` | `80` | max route-history entries |
| `route_history_ttl_sec` | `300` | route-history entry lifetime |
| `provider_metadata_ttl_sec` | `300` | per-model endpoint cache lifetime |
| `privacy_policy_ttl_sec` | `21600` | policy cache lifetime and refresh-loop sleep floor |
| `privacy_fail_closed` | `false` | treat missing/unknown privacy flags as forbidden |
| `providers_allow_training` | `false` | allow providers that train on prompts |
| `providers_allow_training_openrouter` | `false` | allow providers whose OpenRouter training flag is set |
| `providers_allow_retains_prompts` | `true` | allow providers that retain prompts |
| `providers_allow_can_publish` | `false` | allow providers that may publish prompts |
| `provider_require_capabilities` | `[]` | required provider capabilities |
| `provider_only_max` | `10` | max providers sent in `provider.only` |
| `upstream_timeout_sec` | `300` | upstream timeout; total for non-streaming, read timeout for streaming |
| `fill_defaults_if_absent` | `{}` | fields set only when absent |
| `force_body_overrides` | `{}` | fields force-set on the outbound body |
| `prompt_cache` | `{"enabled": false, "ttl_sec": 300}` | upstream prompt-cache headers |
| `per_model_overrides` | `{}` | per-model shallow settings overrides |

Semantics:

- **Weights** are sum-normalized over positive weights whose metric is present.
- **Percentile bands** are averaged; a missing band makes the metric absent, which falls back to the cross-provider median.
- **Privacy gates** are fail-open by default: only an explicit `true` policy flag is rejected, and only when the matching `providers_allow_*` key is `false`. Missing policy, unknown slug, or null flag is not a forbid. With `privacy_fail_closed: true` only an explicit `false` flag passes; missing policy, unknown slug, and null flag all drop as `privacy:<flag>`.
- **`provider_require_capabilities`** — when non-empty, a provider must list every capability; unknown counts as missing.
- **`per_model_overrides`** is a shallow `.update(...)` over the merged settings, so nested dicts are replaced whole (unlike the base deep merge).
- **`force_body_overrides`** then **`fill_defaults_if_absent`** are applied after `provider.only` is set.
- **`prompt_cache.enabled`** adds `X-OpenRouter-Cache: true`; `ttl_sec` adds `X-OpenRouter-Cache-TTL`.
- **`upstream_timeout_sec`** maps per transport: streaming has no total deadline (only `sock_read`); non-streaming enforces a total. Connect timeout is fixed at 30 s.

## API

| method | path | result |
|---|---|---|
| `GET` | `/health` | `status`, `upstream_base`, `upstream_key_configured`, `policy_loaded`, `models_cached` |
| `POST` | `/chat/completions`, `/v1/chat/completions` | chat completion relay |
| `POST` | `/responses`, `/v1/responses` | responses-API relay |

## Errors

Uniform envelope: `{"error": {"message", "type": "proxy_error", "code"}}`.

| status | code | cause |
|---|---|---|
| 400 | `invalid_request_error` | bad JSON, non-object body, missing `model`, or body read failure |
| 413 | `invalid_request_error` | body exceeds the 96 MiB cap |
| 500 | `missing_credentials` | no `UPSTREAM_API_KEY` |
| 503 | `metadata_failure` | cold metadata miss; retry shortly |
| 404 | `no_eligible_provider` | no provider passes constraints; drops in message |
| 504 | `upstream_timeout` | upstream connect/read timeout or stalled stream |
| 502 | `upstream_error` | upstream connection failed |
| 400 | `request_cancelled` | client disconnected before upstream responded |

## Observability

- Logs: `_temp_/temp.log` plus stdout.
- `route` lines carry `op`, `model`, `stream`, `candidates`, `eligible`, `drops`, `only`, `overlap`, `stick_boost`, `served`, `served_requested`, `result`, `upstream_status`, `ms`.
- Never logged: API keys, auth headers, prompt bodies.
