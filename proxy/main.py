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
    "stations": [],      # merged, flat records ready for filtering
    "updated_at": None,
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


async def fetch_paged(client: httpx.AsyncClient, path: str, token: str, dump_first: bool) -> list[dict]:
    out: list[dict] = []
    batch = 1
    while True:
        if batch > 1:
            await asyncio.sleep(REQUEST_SPACING)
        r = await client.get(
            f"{FUEL_HOST}{path}",
            params={"batch-number": batch},
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


def _coerce_station(s: dict) -> dict | None:
    if s.get("permanent_closure") or s.get("temporary_closure"):
        return None
    loc = s.get("location") or {}
    if not isinstance(loc, dict):
        return None
    lat = _pluck(loc, "latitude", "lat", "Latitude")
    lon = _pluck(loc, "longitude", "lng", "lon", "Longitude")
    if lat is None or lon is None:
        return None
    town = _pluck(loc, "town", "locality", "post_town", "city", default="") or ""
    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        return None
    return {
        "node_id": s.get("node_id"),
        "name": (s.get("trading_name") or s.get("brand_name") or "").strip(),
        "town": str(town).strip(),
        "lat": lat,
        "lon": lon,
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
        log.info("Cache refresh starting (proxy=%s)", "on" if FUEL_FINDER_PROXY else "off")
        async with httpx.AsyncClient(proxy=FUEL_FINDER_PROXY, trust_env=False) as client:
            token = await get_access_token(client)
            stations_raw = await fetch_paged(client, "/api/v1/pfs", token, dump_first=True)
            await asyncio.sleep(REQUEST_SPACING)
            prices_raw = await fetch_paged(client, "/api/v1/pfs/fuel-prices", token, dump_first=True)

        prices_by_node: dict[Any, list[dict]] = {}
        for p in prices_raw:
            nid = p.get("node_id")
            if nid is None:
                continue
            prices_by_node[nid] = p.get("fuel_prices") or []

        merged: list[dict] = []
        with_e10 = 0
        for s in stations_raw:
            rec = _coerce_station(s)
            if rec is None:
                continue
            e10, b7 = _coerce_prices(prices_by_node.get(rec["node_id"], []))
            rec["e10"] = e10
            rec["b7"] = b7
            if e10 is not None:
                with_e10 += 1
            merged.append(rec)

        _cache["stations"] = merged
        _cache["updated_at"] = int(time.time())
        _cache["last_error"] = None
        log.info(
            "Cache refresh complete: %d stations (%d with E10) in %.1fs",
            len(merged), with_e10, time.time() - started,
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
    for s in _cache["stations"]:
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
        "town": best["town"],
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
