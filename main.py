"""
Frontier Weather Labs – Flask backend
Serves the landing page, the LAMP viewer, and the NBM viewer,
plus all API routes consumed by the client-side JavaScript.
"""

import logging
import os
import threading
import time
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, Response, jsonify, request, send_file

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Simple thread-safe in-memory cache
# ---------------------------------------------------------------------------

_cache: dict = {}
_cache_lock = threading.Lock()


def _cache_get(key: str):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.monotonic() < entry["expires"]:
            return entry["value"]
    return None


def _cache_set(key: str, value, ttl: int):
    with _cache_lock:
        _cache[key] = {"value": value, "expires": time.monotonic() + ttl}


# ---------------------------------------------------------------------------
# TTLs (seconds)
# ---------------------------------------------------------------------------

LAMP_BULLETIN_TTL = 3600        # full bulletin cached for 1 hour
LAMP_RUNS_TTL = 3600
NBM_TTL = 3600
METAR_TTL = 300                 # METARs refresh every 5 minutes
TIMEZONE_TTL = 86400            # timezone lookups cached for 1 day

# ---------------------------------------------------------------------------
# NOAA / external data URLs
# ---------------------------------------------------------------------------

# LAMP – MDL text bulletin for a single station
# Returns the raw "GFS LAMP" fixed-width text block the JS parser expects.
LAMP_STATION_URL = "https://www.mdl.noaa.gov/lamp/lampwww/getdata.php"

# NBM text guidance (NBH / NBS / NBE / NBX) for a single station
# NOAA distributes these via the Telecommunications Gateway.
NBM_STATION_URL = "https://tgftp.nws.noaa.gov/data/forecasts/nbm/point/{product}/{icao_lower}.{product_lower}"

# METAR – Aviation Weather Center JSON API
AWC_METAR_URL = "https://aviationweather.gov/api/data/metar"

# Timezone lookup – Open-Meteo geocoding + timezone (no key required)
OPEN_METEO_TZ_URL = "https://api.open-meteo.com/v1/forecast"

# ---------------------------------------------------------------------------
# HTML pages
# ---------------------------------------------------------------------------


@app.route("/")
def index():
    return send_file("index.html")


@app.route("/lamp")
def lamp():
    return send_file("lamp.html")


@app.route("/nbm")
def nbm():
    return send_file("nbm.html")


# ---------------------------------------------------------------------------
# LAMP API
# ---------------------------------------------------------------------------


def _fetch_lamp_bulletin(icao: str, run_time: str | None = None) -> tuple[str | None, str | None]:
    """
    Fetch the GFS LAMP text bulletin for *icao* from NOAA MDL.
    Returns (bulletin_text, model_run_time_str) or (None, None) on failure.
    model_run_time_str is YYYYMMDDHH.
    """
    params = {
        "type": "lamp",
        "icao": icao.upper(),
        "days": 2,
    }
    if run_time:
        params["cycle"] = run_time  # YYYYMMDDHH

    cache_key = f"lamp:{icao.upper()}:{run_time or 'latest'}"
    cached = _cache_get(cache_key)
    if cached:
        return cached["text"], cached["run_time"]

    try:
        resp = requests.get(LAMP_STATION_URL, params=params, timeout=15)
        resp.raise_for_status()
        text = resp.text

        # Derive the model run time from the bulletin header if not explicit.
        # LAMP header lines look like:
        #   KCVG   CINCINNATI/NORTHER GFS LAMP GUIDANCE  4/16/2026  0600 UTC
        model_run_time = run_time
        if not model_run_time:
            for line in text.splitlines():
                if "GFS LAMP GUIDANCE" in line and "UTC" in line:
                    # Parse "4/16/2026  0600 UTC" at end of line
                    try:
                        parts = line.strip().split()
                        # last two tokens: "0600" "UTC"
                        utc_hour = parts[-2][:2]           # "06"
                        date_token = parts[-3]             # "4/16/2026"
                        m, d, y = date_token.split("/")
                        model_run_time = f"{y}{int(m):02d}{int(d):02d}{utc_hour}"
                    except Exception:
                        pass
                    break

        _cache_set(cache_key, {"text": text, "run_time": model_run_time}, LAMP_BULLETIN_TTL)
        return text, model_run_time

    except Exception as exc:
        log.error("LAMP fetch failed for %s: %s", icao, exc)
        return None, None


@app.route("/api/lamp")
def api_lamp():
    """
    GET /api/lamp?icao=XXXX[&run_time=YYYYMMDDHH]
    Returns the raw GFS LAMP text bulletin for the requested station.
    Sets X-Model-Run-Time response header (YYYYMMDDHH).
    """
    icao = (request.args.get("icao", "") or "").strip().upper()
    if len(icao) != 4:
        return jsonify({"error": "Missing or invalid icao parameter"}), 400

    run_time = (request.args.get("run_time", "") or "").strip() or None

    text, model_run_time = _fetch_lamp_bulletin(icao, run_time)
    if text is None:
        return jsonify({"error": f"Could not retrieve LAMP data for {icao}"}), 502

    headers = {}
    if model_run_time:
        headers["X-Model-Run-Time"] = model_run_time
    headers["Content-Type"] = "text/plain; charset=utf-8"

    return Response(text, status=200, headers=headers)


@app.route("/api/lamp/runs")
def api_lamp_runs():
    """
    GET /api/lamp/runs
    Returns JSON {runs: [{display, is_synoptic, model_run_time}]}
    listing the most recent available LAMP model runs (up to 7).
    """
    cache_key = "lamp:runs"
    cached = _cache_get(cache_key)
    if cached:
        return jsonify(cached)

    now_utc = datetime.now(timezone.utc)
    runs = []
    for hours_ago in range(7):
        run_dt = now_utc - timedelta(hours=hours_ago)
        run_dt = run_dt.replace(minute=0, second=0, microsecond=0)
        hour_str = run_dt.strftime("%H")
        date_str = run_dt.strftime("%Y-%m-%d")
        model_run_time = run_dt.strftime("%Y%m%d%H")
        # LAMP synoptic runs are at 00, 06, 12, 18 Z
        is_synoptic = run_dt.hour % 6 == 0
        display = f"{hour_str}Z {date_str}"
        runs.append({
            "display": display,
            "is_synoptic": is_synoptic,
            "model_run_time": model_run_time,
        })

    result = {"runs": runs}
    _cache_set(cache_key, result, LAMP_RUNS_TTL)
    return jsonify(result)


# ---------------------------------------------------------------------------
# NBM API
# ---------------------------------------------------------------------------


def _fetch_nbm_bulletin(icao: str, product: str) -> tuple[str | None, str | None]:
    """
    Fetch an NBM text bulletin (NBH / NBS / NBE / NBX) from NOAA.
    Returns (bulletin_text, run_time_str) or (None, None) on failure.
    """
    prod = product.upper()
    if prod not in ("NBH", "NBS", "NBE", "NBX"):
        return None, None

    cache_key = f"nbm:{icao.upper()}:{prod}"
    cached = _cache_get(cache_key)
    if cached:
        return cached["text"], cached["run_time"]

    # NOAA Telecommunications Gateway path for NBM point forecasts.
    # e.g. https://tgftp.nws.noaa.gov/data/forecasts/nbm/point/nbh/kmci.nbh
    url = (
        f"https://tgftp.nws.noaa.gov/data/forecasts/nbm/point/"
        f"{prod.lower()}/{icao.lower()}.{prod.lower()}"
    )

    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        text = resp.text

        # Parse run time from NBM header line, e.g.:
        #   KMCI   NBM V4.3 NBH GUIDANCE    4/07/2026  1800 UTC
        run_time = None
        for line in text.splitlines():
            if "NBM" in line and "GUIDANCE" in line and "UTC" in line:
                try:
                    parts = line.strip().split()
                    utc_hour = parts[-2][:2]
                    date_token = parts[-3]
                    m, d, y = date_token.split("/")
                    run_time = f"{y}{int(m):02d}{int(d):02d}{utc_hour}"
                except Exception:
                    pass
                break

        _cache_set(cache_key, {"text": text, "run_time": run_time}, NBM_TTL)
        return text, run_time

    except Exception as exc:
        log.error("NBM fetch failed for %s %s: %s", icao, prod, exc)
        return None, None


@app.route("/api/nbm")
def api_nbm():
    """
    GET /api/nbm?icao=XXXX&product=NBH
    Returns raw NBM text bulletin for the station.
    Sets X-NBM-Run-Time response header (YYYYMMDDHH).
    """
    icao = (request.args.get("icao", "") or "").strip().upper()
    product = (request.args.get("product", "NBH") or "NBH").strip().upper()

    if len(icao) != 4:
        return jsonify({"error": "Missing or invalid icao parameter"}), 400
    if product not in ("NBH", "NBS", "NBE", "NBX"):
        return jsonify({"error": f"Unknown NBM product: {product}"}), 400

    text, run_time = _fetch_nbm_bulletin(icao, product)
    if text is None:
        return jsonify({"error": f"Could not retrieve NBM {product} data for {icao}"}), 502

    headers = {"Content-Type": "text/plain; charset=utf-8"}
    if run_time:
        headers["X-NBM-Run-Time"] = run_time

    return Response(text, status=200, headers=headers)


# ---------------------------------------------------------------------------
# METAR API
# ---------------------------------------------------------------------------


def _fetch_metar(icao: str) -> dict | None:
    """
    Fetch the latest METAR for *icao* via the Aviation Weather Center JSON API.
    Returns a normalised dict the JS frontend expects, or None on failure.
    """
    cache_key = f"metar:{icao.upper()}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        resp = requests.get(
            AWC_METAR_URL,
            params={"ids": icao.upper(), "format": "json", "taf": "false"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        if not data:
            _cache_set(cache_key, None, METAR_TTL)
            return None

        obs = data[0]

        # Determine flight category
        vis = obs.get("visib")
        ceiling = None
        sky_cover_str = None
        for layer in (obs.get("clouds") or []):
            cover = layer.get("cover", "")
            base = layer.get("base")
            if cover in ("BKN", "OVC", "VV") and base is not None:
                if ceiling is None or base < ceiling:
                    ceiling = base
            sky_cover_str = cover  # last layer wins for display

        if ceiling is None:
            ceiling_ft = None
        else:
            ceiling_ft = ceiling

        # Flight category
        vis_f = float(vis) if vis is not None else None
        cat = "VFR"
        if ceiling_ft is not None or vis_f is not None:
            if (ceiling_ft is not None and ceiling_ft < 500) or (vis_f is not None and vis_f < 1):
                cat = "LIFR"
            elif (ceiling_ft is not None and ceiling_ft < 1000) or (vis_f is not None and vis_f < 3):
                cat = "IFR"
            elif (ceiling_ft is not None and ceiling_ft < 3000) or (vis_f is not None and vis_f < 5):
                cat = "MVFR"

        # Relative humidity (August-Roche-Magnus approximation)
        rh = None
        temp_c = obs.get("temp")
        dewp_c = obs.get("dewp")
        if temp_c is not None and dewp_c is not None:
            rh = round(100 * (
                (17.625 * dewp_c / (243.04 + dewp_c)) -
                (17.625 * temp_c / (243.04 + temp_c))
            ) ** 0 *
            (2.71828 ** (17.625 * dewp_c / (243.04 + dewp_c))) /
            (2.71828 ** (17.625 * temp_c / (243.04 + temp_c))))
            # simpler formula
            rh = round(100 * (2.71828 ** (17.625 * dewp_c / (243.04 + dewp_c))) /
                       (2.71828 ** (17.625 * temp_c / (243.04 + temp_c))))

        result = {
            "flight_category": cat,
            "ceiling": ceiling_ft,
            "visibility": vis_f,
            "sky_cover": sky_cover_str,
            "temp": temp_c,
            "dewpoint": dewp_c,
            "wind_dir": obs.get("wdir"),
            "wind_speed": obs.get("wspd"),
            "wind_gust": obs.get("wgst"),
            "relative_humidity": rh,
        }

        _cache_set(cache_key, result, METAR_TTL)
        return result

    except Exception as exc:
        log.error("METAR fetch failed for %s: %s", icao, exc)
        return None


@app.route("/api/metar")
def api_metar():
    """
    GET /api/metar?icao=XXXX
    Returns JSON with the latest METAR observation fields.
    """
    icao = (request.args.get("icao", "") or "").strip().upper()
    if len(icao) != 4:
        return jsonify({"error": "Missing or invalid icao parameter"}), 400

    obs = _fetch_metar(icao)
    if obs is None:
        return jsonify({"error": f"No METAR data for {icao}"}), 404

    return jsonify(obs)


# ---------------------------------------------------------------------------
# Timezone API
# ---------------------------------------------------------------------------


@app.route("/api/timezone")
def api_timezone():
    """
    GET /api/timezone?lat=XX.XX&lon=YY.YY
    Returns JSON {timezone: "America/Chicago", offset_hours: -5.0}
    Uses Open-Meteo (free, no API key) for the timezone lookup.
    """
    try:
        lat = float(request.args.get("lat", ""))
        lon = float(request.args.get("lon", ""))
    except (TypeError, ValueError):
        return jsonify({"error": "lat and lon must be numeric"}), 400

    cache_key = f"tz:{lat:.3f}:{lon:.3f}"
    cached = _cache_get(cache_key)
    if cached:
        return jsonify(cached)

    try:
        resp = requests.get(
            OPEN_METEO_TZ_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "timezone": "auto",
                "current_weather": "false",
                "forecast_days": 1,
                "hourly": "temperature_2m",  # minimal field to satisfy API
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        tz_name = data.get("timezone", "UTC")
        utc_offset_seconds = data.get("utc_offset_seconds", 0)
        offset_hours = utc_offset_seconds / 3600

        result = {
            "timezone": tz_name,
            "offset_hours": offset_hours,
        }
        _cache_set(cache_key, result, TIMEZONE_TTL)
        return jsonify(result)

    except Exception as exc:
        log.error("Timezone lookup failed for %.3f,%.3f: %s", lat, lon, exc)
        return jsonify({"error": "Timezone lookup failed"}), 502


# ---------------------------------------------------------------------------
# Gunicorn entry-point
# Run locally:  python main.py
# Production:   gunicorn main:app --workers 4 --bind 0.0.0.0:8000
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
