import asyncio
import json
import logging
import math
import os
import time
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("fuelproxy")

FUEL_HOST = "https://www.fuel-finder.service.gov.uk"
CLIENT_ID = os.environ.get("FUEL_FINDER_CLIENT_ID")
CLIENT_SECRET = os.environ.get("FUEL_FINDER_CLIENT_SECRET")
# HTTP(S) proxy URL for Fuel Finder calls — required when running outside the
# UK because CloudFront geo-blocks non-UK egress. Format:
# http://user:pass@host:port
FUEL_FINDER_PROXY = os.environ.get("FUEL_FINDER_PROXY") or None

REQUEST_SPACING = 2.2          # 30 RPM live limit -> 2.0s, pad to 2.2s
REFRESH_INTERVAL = 3600        # hourly full refresh
BATCH_SIZE = 500
FIRST_REQUEST_WAIT = 120       # seconds a cold request will wait for initial load

_cache: dict[str, Any] = {
    "stations": {},      # dict keyed by node_id -> flat record
    "updated_at": None,  # unix ts of last successful refresh (full or incremental)
    "refreshing": False,
    "last_error": None,
}


# --- Fuel Finder client ---------------------------------------------------

UA = "fuelstation-proxy/0.1 (+https://github.com/TheHacksaw/fuelstation)"


async def get_access_token(client: httpx.AsyncClient) -> str:
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError("FUEL_FINDER_CLIENT_ID / FUEL_FINDER_CLIENT_SECRET not set")
    r = await client.post(
        f"{FUEL_HOST}/api/v1/oauth/generate_access_token",
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "fuelfinder.read",
        },
        headers={"Accept": "application/json", "User-Agent": UA},
        timeout=30.0,
    )
    if r.status_code >= 400:
        log.error(
            "OAuth %d response headers=%s body=%s",
            r.status_code,
            dict(r.headers),
            r.text[:1000],
        )
        r.raise_for_status()
    body = r.json()
    if isinstance(body, dict) and isinstance(body.get("data"), dict):
        body = body["data"]
    token = body.get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in OAuth response: {body}")
    return token


async def fetch_paged(
    client: httpx.AsyncClient,
    path: str,
    token: str,
    dump_first: bool,
    extra_params: dict | None = None,
) -> list[dict]:
    out: list[dict] = []
    batch = 1
    while True:
        if batch > 1:
            await asyncio.sleep(REQUEST_SPACING)
        params = {"batch-number": batch}
        if extra_params:
            params.update(extra_params)
        r = await client.get(
            f"{FUEL_HOST}{path}",
            params=params,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "User-Agent": UA,
            },
            timeout=60.0,
        )
        r.raise_for_status()
        items = r.json()
        if not isinstance(items, list):
            raise RuntimeError(f"Expected array at {path}, got {type(items).__name__}")
        if dump_first and batch == 1 and items:
            log.info("SAMPLE %s item: %s", path, json.dumps(items[0], indent=2)[:2000])
        out.extend(items)
        log.info("%s batch %d -> %d items (running total %d)", path, batch, len(items), len(out))
        if len(items) < BATCH_SIZE:
            return out
        batch += 1


# --- schema coercion ------------------------------------------------------
# Exact field names inside `location` and `fuel_prices[]` aren't public-documented.
# We probe several likely names and log a warning if none match, so the first
# live response will tell us what to harden on.

def _pluck(d: dict, *keys, default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def _is_closed(raw: dict) -> bool:
    return bool(raw.get("permanent_closure")) or bool(raw.get("temporary_closure"))


def _coerce_station(s: dict) -> dict | None:
    loc = s.get("location") or {}
    if not isinstance(loc, dict):
        return None
    lat = _pluck(loc, "latitude", "lat", "Latitude")
    lon = _pluck(loc, "longitude", "lng", "lon", "Longitude")
    if lat is None or lon is None:
        return None
    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        return None
    line1 = str(_pluck(loc, "address_line_1", default="")).strip()
    line2 = str(_pluck(loc, "address_line_2", default="")).strip()
    address = ", ".join(p for p in (line1, line2) if p)
    return {
        "node_id": s.get("node_id"),
        "name": (s.get("trading_name") or s.get("brand_name") or "").strip(),
        "brand": (s.get("brand_name") or "").strip(),
        "town": str(_pluck(loc, "city", "town", "locality", "post_town", default="")).strip().title(),
        "postcode": str(_pluck(loc, "postcode", default="")).strip().upper(),
        "address": address.title(),
        "lat": lat,
        "lon": lon,
        "is_motorway": bool(s.get("is_motorway_service_station")),
        "is_supermarket": bool(s.get("is_supermarket_service_station")),
        "e10": None,
        "b7": None,
    }


def _coerce_prices(fuel_prices: list[dict]) -> tuple[float | None, float | None]:
    """Return (e10_ppl, b7_ppl) in pence per litre."""
    e10 = b7 = None
    for fp in fuel_prices or []:
        code = str(_pluck(fp, "fuel_type", "code", "type", default="")).upper()
        price = _pluck(fp, "price", "price_per_litre", "pence_per_litre", "amount")
        if price is None:
            continue
        try:
            price = float(price)
        except (TypeError, ValueError):
            continue
        # Some APIs return pounds (1.399) rather than pence (139.9). Normalise to pence.
        if price < 10:
            price *= 100
        if "E10" in code or "UNLEADED" in code:
            if e10 is None or price < e10:
                e10 = price
        elif "B7" in code or "DIESEL" in code:
            if b7 is None or price < b7:
                b7 = price
    return e10, b7


# --- refresh --------------------------------------------------------------

async def refresh_cache() -> None:
    if _cache["refreshing"]:
        log.info("Refresh already in progress, skipping")
        return
    _cache["refreshing"] = True
    started = time.time()
    try:
        is_full = not _cache["stations"] or _cache["updated_at"] is None
        extra_params: dict | None = None
        if not is_full:
            # Subtract a safety margin so we don't miss records whose effective
            # timestamp is a few seconds before ours (clock skew, API lag).
            since_ts = int(_cache["updated_at"]) - 300
            extra_params = {
                "effective-start-timestamp": time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.gmtime(since_ts)
                )
            }
        mode = "full" if is_full else f"incremental since {extra_params['effective-start-timestamp']}"
        log.info("Cache refresh starting (%s, proxy=%s)", mode, "on" if FUEL_FINDER_PROXY else "off")

        async with httpx.AsyncClient(proxy=FUEL_FINDER_PROXY, trust_env=False) as client:
            token = await get_access_token(client)
            stations_raw = await fetch_paged(
                client, "/api/v1/pfs", token, dump_first=is_full, extra_params=extra_params
            )
            await asyncio.sleep(REQUEST_SPACING)
            prices_raw = await fetch_paged(
                client, "/api/v1/pfs/fuel-prices", token, dump_first=is_full, extra_params=extra_params
            )

        stations: dict[Any, dict] = {} if is_full else dict(_cache["stations"])
        for s in stations_raw:
            nid = s.get("node_id")
            if not nid:
                continue
            if _is_closed(s):
                stations.pop(nid, None)
                continue
            rec = _coerce_station(s)
            if rec is None:
                continue
            existing = stations.get(nid)
            if existing:
                # Station record updated but prices come from the other endpoint —
                # preserve what we already have until the prices merge below.
                rec["e10"] = existing["e10"]
                rec["b7"] = existing["b7"]
            stations[nid] = rec

        prices_updated = 0
        for p in prices_raw:
            nid = p.get("node_id")
            if nid is None:
                continue
            station = stations.get(nid)
            if station is None:
                continue
            e10, b7 = _coerce_prices(p.get("fuel_prices") or [])
            if e10 is not None:
                station["e10"] = e10
            if b7 is not None:
                station["b7"] = b7
            if e10 is not None or b7 is not None:
                prices_updated += 1

        _cache["stations"] = stations
        _cache["updated_at"] = int(time.time())
        _cache["last_error"] = None
        with_e10 = sum(1 for s in stations.values() if s["e10"] is not None)
        log.info(
            "Refresh done (%s): total=%d (E10=%d) — this run: stations_touched=%d prices_touched=%d in %.1fs",
            mode, len(stations), with_e10, len(stations_raw), prices_updated, time.time() - started,
        )
    except Exception as e:
        log.exception("Cache refresh failed")
        _cache["last_error"] = repr(e)
    finally:
        _cache["refreshing"] = False


async def refresh_loop() -> None:
    while True:
        await refresh_cache()
        await asyncio.sleep(REFRESH_INTERVAL)


# --- geocoding ------------------------------------------------------------

async def geocode(postcode: str) -> tuple[float, float, str]:
    pc = "".join(postcode.split()).upper()
    if not pc:
        raise HTTPException(400, "Empty postcode")
    async with httpx.AsyncClient(trust_env=False) as client:
        r = await client.get(f"https://api.postcodes.io/postcodes/{pc}", timeout=10.0)
    if r.status_code == 404:
        raise HTTPException(400, f"Unknown postcode: {postcode}")
    r.raise_for_status()
    result = r.json().get("result") or {}
    lat, lon = result.get("latitude"), result.get("longitude")
    if lat is None or lon is None:
        raise HTTPException(400, f"Postcode has no coordinates: {postcode}")
    return float(lat), float(lon), result.get("admin_district") or ""


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3958.7613
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


# --- FastAPI app ----------------------------------------------------------

@asynccontextmanager
async def lifespan(_app: FastAPI):
    task = asyncio.create_task(refresh_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass


app = FastAPI(lifespan=lifespan, title="Fuel Finder Proxy")


async def _wait_for_cache() -> None:
    if _cache["stations"]:
        return
    for _ in range(FIRST_REQUEST_WAIT):
        await asyncio.sleep(1)
        if _cache["stations"]:
            return
    raise HTTPException(503, "Cache warming up, try again shortly")


@app.get("/cheapest")
async def cheapest(
    postcode: str = Query(..., min_length=2, max_length=10),
    radius_miles: float = Query(10.0, gt=0, le=100),
):
    await _wait_for_cache()
    lat, lon, _ = await geocode(postcode)

    best = None
    best_dist = None
    for s in _cache["stations"].values():
        if s["e10"] is None:
            continue
        d = haversine_miles(lat, lon, s["lat"], s["lon"])
        if d > radius_miles:
            continue
        if best is None or s["e10"] < best["e10"]:
            best, best_dist = s, d

    if best is None:
        raise HTTPException(404, f"No E10 prices within {radius_miles} mi of {postcode}")

    return {
        "name": best["name"],
        "brand": best["brand"],
        "town": best["town"],
        "postcode": best["postcode"],
        "address": best["address"],
        "is_motorway": best["is_motorway"],
        "is_supermarket": best["is_supermarket"],
        "e10": round(best["e10"], 1),
        "b7": round(best["b7"], 1) if best["b7"] is not None else None,
        "distance": round(best_dist, 1),
        "updated_at": _cache["updated_at"],
    }


@app.get("/health")
async def health():
    return {
        "stations": len(_cache["stations"]),
        "updated_at": _cache["updated_at"],
        "refreshing": _cache["refreshing"],
        "last_error": _cache["last_error"],
    }
