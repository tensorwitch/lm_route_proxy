# lm_route_proxy

openrouter, but the routing decision is local.

a small aiohttp proxy between an openai-compatible client and openrouter. per request it reads live endpoint stats (latency, tps, price) and prompt-prefix cache affinity, ranks candidates, and sends openrouter an ordered `provider.only` list. the rest of the body passes through untouched. openrouter executes the fallback.

## why

openrouter provider-sorting sucks.

## what it isn't

- not a retry layer, not a model router. model strings pass through verbatim.
- not a billing oracle. price is a ranking signal, not an invoice.
- not stateful. restart drops route history and metadata caches.

## run

```bash (tmux)
cp .env.example .env    # set UPSTREAM_API_KEY (overwrites client (make dummy))
python3 core.py         # http://127.0.0.1:9354
```

client base_url → `http://127.0.0.1:9354` instead of `https://openrouter.ai/api/v1`.

```docker (recom)
docker compose -f docker/docker-compose.yml up --build
```

## how it picks

1. **canonicalize** — request → prompt chunks + mode fingerprint (reasoning knobs etc). different mode, never compared.
2. **drop** — unknown providers, client `provider.*` constraints, missing capabilities, privacy-flag violations. reasons in `debug.drops`.
3. **score** — latency↓ tps↑ price↓, each clipped 0–1 by 5/95 quantiles. missing value → cross-provider median.
4. **stick** — a recent request sharing enough prefix boosts its provider. cache affinity is a cost signal.
5. **rank** — weighted mean → top `provider_only_max` slugs. that order is the fallback list.

## layout

- `core.py` — server, I/O, orchestration. reaches routing only via `decide_route(...)` + `RouteHistory`.
- `routing.py` — pure policy. never touches the wire.
- `MANUAL.md` — config keys, api, error codes.
- `settings.json` knobs (hot-reloaded, comments ok).

## ...

- body cap 96 MiB → 413. mod for multi-image - keep for low footprint. 
- client `provider.allow_fallbacks: false` is overwritten. get `false` effect with `"provider_only_max": 1`.
- disconnect detection is polled, not event-driven.
- logs in `_temp_/temp.log`. prompts and keys never logged.
