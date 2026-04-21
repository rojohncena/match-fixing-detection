#!/usr/bin/env python3
"""
Call API-Football (api-sports.io) to print when Romania SuperLiga
AFC Unirea Slobozia vs. ASC Oțelul Galați on 2026-03-23 started (UTC kickoff).

Requires an API key from https://www.api-football.com/ (dashboard).
Set API_FOOTBALL_KEY or APISPORTS_KEY in the environment, or put API_FOOTBALL_KEY=... in a .env file beside this script (loaded automatically).
"""

from __future__ import annotations

import json
import os
import ssl
import unicodedata
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE_URL = "https://v3.football.api-sports.io"
MATCH_DATE = "2026-03-23"
# API-Football "season" is the year the competition starts (e.g. Aug–May 2025/26 → 2025).
# Free plans only allow 2022–2024; if unset, we try a sensible year then fall back.
SEASON: int | None = None

HOME_HINTS = ("unirea", "slobozia")
AWAY_HINTS = ("otelul", "galati")


def _load_dotenv() -> None:
    """Load KEY=value pairs from .env next to this script (Python has no built-in .env support)."""
    path = Path(__file__).resolve().parent / ".env"
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


def _ascii_lower(s: str) -> str:
    return (
        unicodedata.normalize("NFKD", s)
        .encode("ascii", "ignore")
        .decode()
        .lower()
    )


def _api_season_for_kickoff_date(match_date: str) -> int:
    y, m, _ = map(int, match_date.split("-"))
    # Jan–Jul: still in the season that began the previous August.
    return y if m >= 8 else y - 1


def _season_try_list() -> list[int]:
    preferred = SEASON if SEASON is not None else _api_season_for_kickoff_date(MATCH_DATE)
    out: list[int] = []
    for s in (preferred, preferred - 1, 2024, 2023, 2022):
        if s >= 2010 and s not in out:
            out.append(s)
    return out


def _api_get(path: str, params: dict) -> dict:
    key = os.environ.get("API_FOOTBALL_KEY") or os.environ.get("APISPORTS_KEY")
    if not key:
        raise SystemExit(
            "Missing API key. Set API_FOOTBALL_KEY (or APISPORTS_KEY) to your api-football key."
        )
    query = urlencode(params)
    url = f"{BASE_URL}{path}?{query}"
    req = Request(url, headers={"x-apisports-key": key})
    ctx = ssl.create_default_context()
    try:
        with urlopen(req, context=ctx, timeout=30) as resp:
            return json.load(resp)
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise SystemExit(f"HTTP {e.code} for {url}\n{body}") from e
    except URLError as e:
        raise SystemExit(f"Request failed: {e}") from e


def _find_romania_superliga_league_id() -> tuple[int, int]:
    last_err: dict | str | None = None
    for season in _season_try_list():
        data = _api_get("/leagues", {"country": "Romania", "season": season})
        err = data.get("errors")
        results = data.get("response") or []
        if err:
            last_err = err
        if results:
            league_id = _pick_top_romanian_league_id(results)
            print(f"Using league id {league_id} (season={season} for /leagues request)", flush=True)
            return league_id, season
    msg = f"No Romanian leagues returned after trying seasons {_season_try_list()}."
    if last_err:
        msg += f"\nLast API errors: {last_err}"
    if isinstance(last_err, dict) and "Free plans" in str(last_err.get("plan", "")):
        msg += (
            "\n\nFree API-Football plans only expose seasons 2022–2024 for many endpoints. "
            "Upgrade the key or set SEASON to a year your plan allows."
        )
    raise SystemExit(msg)


def _pick_top_romanian_league_id(results: list) -> int:
    candidates = []
    for item in results:
        lg = item.get("league") or {}
        lid = lg.get("id")
        name = (lg.get("name") or "").lower()
        if lid is None:
            continue
        if "super" in name or "superliga" in name.replace(" ", ""):
            candidates.append((0, lid, lg.get("name")))
        elif "liga 1" in name or name.strip() == "liga i":
            candidates.append((1, lid, lg.get("name")))
    candidates.sort(key=lambda x: x[0])
    if not candidates:
        names = [((r.get("league") or {}).get("name")) for r in results]
        raise SystemExit(
            "Could not pick Romania top tier. Available league names: "
            + ", ".join(n for n in names if n)
        )
    _, league_id, name = candidates[0]
    print(f"  → {name}", flush=True)
    return int(league_id)


def _teams_match_fixture(home: str, away: str) -> bool:
    h, a = _ascii_lower(home), _ascii_lower(away)
    home_ok = all(part in h for part in HOME_HINTS)
    away_ok = all(part in a for part in AWAY_HINTS)
    swapped = all(part in a for part in HOME_HINTS) and all(part in h for part in AWAY_HINTS)
    return (home_ok and away_ok) or swapped


def main() -> None:
    _load_dotenv()
    league_id, _ = _find_romania_superliga_league_id()
    data: dict | None = None
    last_err: dict | str | None = None
    for season in _season_try_list():
        data = _api_get(
            "/fixtures",
            {"league": league_id, "season": season, "date": MATCH_DATE},
        )
        err = data.get("errors")
        chunk = data.get("response") or []
        if err:
            last_err = err
            continue
        if chunk:
            print(f"Using season={season} for /fixtures (same date as MATCH_DATE)", flush=True)
            break
        print(
            f"No fixtures for league={league_id} season={season} on {MATCH_DATE}; "
            "trying next season candidate…",
            flush=True,
        )
    if data is None:
        raise SystemExit("Unexpected: no fixture request was made.")
    fixtures = data.get("response") or []
    if data.get("errors") and not fixtures:
        msg = f"Could not load fixtures: {data.get('errors')}"
        if last_err and last_err != data.get("errors"):
            msg += f"\n(also saw: {last_err})"
        if isinstance(data.get("errors"), dict) and "Free plans" in str(
            (data.get("errors") or {}).get("plan", "")
        ):
            cal_y = int(MATCH_DATE.split("-")[0])
            api_season_guess = cal_y - 1
            msg += (
                "\n\nYour plan is blocking that season or date. "
                f"A match in calendar year {cal_y} is usually API season {api_season_guess} "
                f"({api_season_guess}/{cal_y}); free keys often only allow 2022–2024. "
                "See dashboard.api-football.com — upgrade the key or change MATCH_DATE to one "
                "your plan allows."
            )
        raise SystemExit(msg)

    matches = []
    for row in fixtures:
        teams = row.get("teams") or {}
        home = (teams.get("home") or {}).get("name") or ""
        away = (teams.get("away") or {}).get("name") or ""
        if _teams_match_fixture(home, away):
            matches.append(row)

    if not matches:
        print(
            f"No fixture on {MATCH_DATE} matched Unirea Slobozia vs Oțelul Galați. "
            f"Returned {len(fixtures)} fixture(s) for that league/date.",
            flush=True,
        )
        for row in fixtures[:15]:
            teams = row.get("teams") or {}
            h = (teams.get("home") or {}).get("name")
            a = (teams.get("away") or {}).get("name")
            print(f"  — {h} vs {a}", flush=True)
        raise SystemExit(1)

    row = matches[0]
    fx = row.get("fixture") or {}
    kickoff = fx.get("date")
    ts = fx.get("timestamp")
    venue = (fx.get("venue") or {}).get("name")
    teams = row.get("teams") or {}
    h = (teams.get("home") or {}).get("name")
    a = (teams.get("away") or {}).get("name")
    print(f"Match: {h} vs {a}", flush=True)
    print(f"Kickoff (API fixture.date, usually UTC ISO): {kickoff}", flush=True)
    if ts is not None:
        print(f"Unix timestamp: {ts}", flush=True)
    if venue:
        print(f"Venue: {venue}", flush=True)


if __name__ == "__main__":
    main()
