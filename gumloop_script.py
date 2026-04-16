"""
Gumloop Run Code Node: Esports Lines to Google Sheets

Fetches esports lines from Underdog Fantasy, PrizePicks, and ParlayPlay,
formats them for Google Sheets output with opening line tracking.
Automatically removes lines for matches that have already started.

INPUTS (from Google Sheets Reader - existing sheet data):
  - existing_event:       list of existing Event values
  - existing_player:      list of existing Player values
  - existing_map:         list of existing Map values
  - existing_stat:        list of existing Stat values
  - existing_pp:          list of existing PrizePicks values
  - existing_pp_opening:  list of existing PrizePicks Opening values
  - existing_ud:          list of existing Underdog values
  - existing_ud_opening:  list of existing Underdog Opening values
OUTPUTS (to Google Sheets Writer):
  - event, player, map_col, stat, prizepicks, prizepicks_opening,
    underdog, underdog_opening, betr, parlayplay, dabble, bovada, mybookie
"""

import requests
import re
import json
import time
import os
import subprocess
import sys
from datetime import datetime, timezone, timedelta


# ---------------------------------------------------------------
# Config
# ---------------------------------------------------------------
UNDERDOG_URL = "https://api.underdogfantasy.com/beta/v5/over_under_lines"
PP_PROXY_URL = "https://pp-python.vercel.app/api/prizepicks"
TARGET_SPORTS = {"LOL", "CS", "DOTA2", "ESPORTS"}

# Google Sheet config for auto-clearing
SPREADSHEET_ID = "10TCKqNaIShIErBlI75sp1YIsU84_ZUPNuqO_ykhLKWc"
SHEET_NAME = "Live Lines"

SERVICE_ACCOUNT_INFO = {
    "type": "service_account",
    "project_id": "serious-mariner-436817-r1",
    "private_key_id": "2b9a2a7bfe88c7fd12eaffca510bab2331bfcbf8",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC5ExxUqXQcsZTA\nTqDJDUWONXNJ74hxRqXK8z3pNaXlvQSxHI0LOnko8bCvU6sP7NyDcLf5JmuxaMA0\nz84pehN+aB51R0nkfnwk+X7r4Tc3QSnVXFVKA2eWF71+LdjrNAKImq1u94lFVl6v\nQHl+qnnWzR6DiCZ1kYi+TSEi1JH/PuUcs8pANJaWNCSfvJ6+TZRaUIY3eV50ihk4\nanysG8br8F66TGf9TivKvCkU317KAsHCmxj0MKAZatxxR0uZWX2h2cdidEw6WVGJ\n8hXk17gUBY2Sk/m2L7TSsTpiJZNR/5+DKF/m00dN2zni+MX7mgFaKqHeDmmq6V2M\nooDdAgDPAgMBAAECggEAMMKWs72DcG3e7X2pwd6jxTBS5BdeQM3mr14/oPo375vJ\nqSoHBP7OXhmzPbYS+xqiDGU9q0BSnkalYSSgwe++RA8Fe0uhbbhfV9R9+oJ3LDEY\nZvSxKdNUzsgJbj2BCZLF2hy6deJ0wZZcUtrvR459lDitgeT0kQQbXVvvz3/myTLj\niD5sKnv8LzMcmt+TBr343rPGJyC13Yhq7y/TmZy3se3D2sVhG800bey9R8gKsu/g\nzFeWpOHJWE8I/2dlovZdNbhqtuRJWcCMnDWvi9aEZ38f7oVBnncxfbsusLNdkZ9r\nHpLZlVhsU3/E/qIXBD+BFmwc6kM6D9Yd40CbPqDHvQKBgQDlumveLGcYdIgYOQk2\nJhsoolJMSJ1o8ldR07P+aBByxtq9MqjchIVCMD33WsaBGKw0ln5jIAj+Jcv0SJye\nhKXGczXgPVPVOrs59avPK2t/uOaHp8u6q3gMFu/6JUvaSNnBsleDsfNxFOQgeH9l\nrDu5+uEgSS//6WkheAq8O2XhfQKBgQDOPWak0M6w7nMrfCUApFb9CX3ciwNLJlCh\n36gqqwYRdBwbHPlp5vSDdmcqcmY5FyKPJ+GCN+4aU2TEnQxOrQVgdhoCznds7GOi\nN42hxdqN+mpjAonp/KhI53P/yBtJ3hmDMmVwyzbE0yxVY+AILy/w+tmi0RWIaJOy\nzB8afrF9OwKBgBvuLm80ttQiVumbBaOvvl2SXq8npPu9eyBXvOqRfG53/uBB6IXn\nFsyVUPNh9gB8H3PFWFh07KL5tXJd4azkM8OM/l/lFOw318uUMu9dOBSvRlf37q0j\na9UMdODU6AQCF3eVV06LtC1rfND11YdnCVvzRKvIOi3DEyUeky+PiTOBAoGABJ0H\nAMTS+s46sUxTn5INiBeAQ0Cw0CuJPjW8k0fEGPvZ7RlW0vGhopcxc5efhcNouH8R\n4lHR97DJ3kQNFG12Y1QA/PMVZNBc4jIP7wB4BRkG7DQQVbWbJhZXV+9n/N0FARRN\nhJpnHTwED9zuFADKN7/Ewomey7BbLXK3d2ZCHiUCgYAytlJ3tPD1+Pdnu0ZtAgHA\niL93zxlCnzbl9NhImznuhljGJVqZdZVa8O0T4EqzaYHJZHrxgrq4m81ArB4ehWHF\ntndLd35YRc2afubiOcSgyIx/uMrdlP5kGO6ABxeOasig2MW9CyNA/14FEkHn/fHk\nIbCwnIsaK4epV8m4Ei9iXg==\n-----END PRIVATE KEY-----\n",
    "client_email": "gumloop-sheets@serious-mariner-436817-r1.iam.gserviceaccount.com",
    "client_id": "102925815886774729057",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/gumloop-sheets%40serious-mariner-436817-r1.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com",
}

GAME_LABELS = {
    "LOL": "LoL",
    "CS2": "CS2",
    "DOTA2": "Dota 2",
    "VAL": "Valorant",
    "COD": "Call of Duty",
    "ESPORTS": "Esports",
}


# ---------------------------------------------------------------
# Fetch Underdog lines
# ---------------------------------------------------------------
def fetch_underdog():
    try:
        resp = requests.get(
            UNDERDOG_URL,
            headers={"Accept": "application/json"},
            timeout=25,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[Underdog] Error fetching: {e}")
        return []

    esports_games = {}
    for g in data.get("games", []):
        if g.get("sport_id") in TARGET_SPORTS:
            esports_games[g["id"]] = {
                "sport_id": g["sport_id"],
                "title": g.get("title", ""),
                "scheduled_at": g.get("scheduled_at", ""),
            }

    app_lookup = {}
    esports_app_ids = set()
    for a in data.get("appearances", []):
        if a.get("match_id") in esports_games:
            esports_app_ids.add(a["id"])
            app_lookup[a["id"]] = {
                "player_id": a.get("player_id", ""),
                "team_name": a.get("team_name", ""),
                "match_id": a["match_id"],
            }

    players = {}
    for p in data.get("players", []):
        name = (p.get("first_name") or "").strip()
        if p.get("last_name"):
            name += " " + p["last_name"]
        players[p["id"]] = name

    rows = []
    for line in data.get("over_under_lines", []):
        ou = line.get("over_under", {})
        app_stat = ou.get("appearance_stat", {})
        app_id = app_stat.get("appearance_id")
        if app_id not in esports_app_ids:
            continue

        app_info = app_lookup[app_id]
        game_info = esports_games.get(app_info["match_id"], {})
        sport_id = game_info.get("sport_id", "")

        player_name = (players.get(app_info["player_id"], "Unknown")).strip()
        player_name = re.sub(
            r"^(?:LoL|LOL|CS2?|DOTA2?|Val|VAL|Valorant):\s*",
            "",
            player_name,
            flags=re.IGNORECASE,
        )

        if sport_id == "CS":
            game_name = "CS2"
        elif sport_id == "ESPORTS":
            title = (game_info.get("title") or "").lower()
            if any(x in title for x in ["cod:", "call of duty", "cdl"]):
                game_name = "COD"
            elif any(x in title for x in ["val:", "valorant", "vct"]):
                game_name = "VAL"
            elif any(x in title for x in ["lpl", "lck", "lec", "lcs", "league", "lol"]):
                game_name = "LOL"
            elif any(x in title for x in ["dota", "ti "]):
                game_name = "DOTA2"
            else:
                game_name = "ESPORTS"
        elif sport_id == "DOTA2":
            game_name = "DOTA2"
        else:
            game_name = sport_id

        options = {}
        for opt in line.get("options", []):
            options[opt["choice"]] = opt.get("american_price", "")

        rows.append({
            "platform": "Underdog",
            "game": game_name,
            "player": player_name,
            "team": app_info.get("team_name", ""),
            "match": game_info.get("title", ""),
            "scheduled": game_info.get("scheduled_at", ""),
            "stat": app_stat.get("display_stat", ""),
            "line": float(line.get("stat_value", 0)),
            "higher_price": options.get("higher", ""),
            "lower_price": options.get("lower", ""),
        })

    # Deduplicate: prefer line with H/L odds
    deduped = []
    seen = {}
    for r in rows:
        key = f"{r['game']}||{r['player']}||{r['stat']}"
        has_odds = r["higher_price"] and r["lower_price"]
        if key not in seen:
            seen[key] = {"index": len(deduped), "has_odds": has_odds}
            deduped.append(r)
        elif has_odds and not seen[key]["has_odds"]:
            deduped[seen[key]["index"]] = r
            seen[key]["has_odds"] = True

    print(f"[Underdog] {len(rows)} raw, {len(deduped)} deduped lines")
    return deduped


# ---------------------------------------------------------------
# Fetch PrizePicks lines (via proxy)
# ---------------------------------------------------------------
def fetch_prizepicks():
    try:
        resp = requests.get(
            PP_PROXY_URL,
            headers={"Accept": "application/json"},
            timeout=35,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data.get("available"):
            print(f"[PrizePicks] Unavailable: {data.get('error', 'unknown')}")
            return []
        lines = data.get("lines", [])
        # Filter to standard odds only (exclude Goblin/Demon)
        standard = [l for l in lines if not l.get("odds_type") or l["odds_type"] == "standard"]
        print(f"[PrizePicks] {len(lines)} total, {len(standard)} standard lines")
        return standard
    except Exception as e:
        print(f"[PrizePicks] Error: {e}")
        return []


# ---------------------------------------------------------------
# Fetch ParlayPlay lines (via Selenium headless scraper)
# ---------------------------------------------------------------

# Map ParlayPlay league names to internal game codes
PLP_LEAGUE_MAP = {
    "valorant": "VAL",
    "lol": "LOL",
    "league of legends": "LOL",
    "cs2": "CS2",
    "counter-strike": "CS2",
    "dota 2": "DOTA2",
    "dota2": "DOTA2",
    "call of duty": "COD",
    "cod": "COD",
}


def _plp_extract_api_data(driver, logs):
    """Extract crossgame/search API response bodies from Chrome performance logs."""
    data = {}
    for entry in logs:
        try:
            msg = json.loads(entry["message"])["message"]
            if msg["method"] == "Network.responseReceived":
                url = msg["params"]["response"]["url"]
                status = msg["params"]["response"]["status"]
                if "crossgame/search" in url and status == 200:
                    request_id = msg["params"]["requestId"]
                    try:
                        body = driver.execute_cdp_cmd(
                            "Network.getResponseBody", {"requestId": request_id}
                        )
                        parsed = json.loads(body["body"])
                        data[url] = parsed
                    except Exception:
                        pass
        except Exception:
            pass
    return data


def _plp_normalize_players(player_entry):
    """
    Normalize a ParlayPlay player entry into standard line format(s).

    Each entry contains nested match/player/stats objects.  The API returns
    FG (Full Game) period data, so we determine the stat prefix from the
    match type (best_of_3 -> "Maps 1-3", etc.).

    Returns a list of dicts (one per stat in the entry).
    """
    results = []
    try:
        match = player_entry.get("match", {})
        sport = match.get("sport", {})
        league = match.get("league", {})

        # Only keep eSports entries
        if sport.get("sportName") != "eSports":
            return results

        league_short = league.get("leagueNameShort", "")
        game = PLP_LEAGUE_MAP.get(league_short.lower(), "ESPORTS")

        player_info = player_entry.get("player", {})
        player_name = player_info.get("fullName", "")
        team_abbr = player_info.get("team", {}).get("teamAbbreviation", "")

        home = match.get("homeTeam", {}).get("teamname", "")
        away = match.get("awayTeam", {}).get("teamname", "")
        match_title = f"{home} vs {away}" if home and away else ""
        match_date = match.get("matchDate", "")

        # FG period: determine map prefix from match type
        match_type = match.get("matchType", "")
        if match_type == "best_of_5":
            prefix = "Maps 1-5 "
        elif match_type == "best_of_3":
            prefix = "Maps 1-3 "
        elif match_type == "best_of_2":
            prefix = "Maps 1-2 "
        else:
            prefix = ""

        for stat in player_entry.get("stats", []):
            stat_name = stat.get("challengeName", "")
            line_val = stat.get("statValue")
            multiplier = stat.get("defaultMultiplier")

            if not stat_name or line_val is None:
                continue

            full_stat = f"{prefix}{stat_name}"

            results.append({
                "platform": "ParlayPlay",
                "game": game,
                "player": player_name,
                "team": team_abbr,
                "match": match_title,
                "scheduled": match_date,
                "stat": full_stat,
                "line": float(line_val),
                "over_multiplier": multiplier,
                "under_multiplier": None,
            })
    except Exception:
        pass
    return results


def fetch_parlayplay():
    """
    Fetch esports lines from ParlayPlay using Selenium headless browser.

    ParlayPlay's API is behind Cloudflare protection, so direct HTTP requests
    are blocked.  This function:
      1. Loads parlayplay.io in headless Chrome to pass Cloudflare.
      2. Intercepts API responses from Chrome performance logs.
      3. Normalises eSports player entries into our standard line format.

    The page automatically fetches ``period=FG`` (Full Game) data on load.
    We map that to the appropriate stat prefix based on match type
    (e.g. best_of_3 -> "Maps 1-3 Kills").

    Returns an empty list gracefully if Selenium/Chrome are not available.
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except ImportError:
        try:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "selenium"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
        except Exception:
            print("[ParlayPlay] Selenium not available, skipping")
            return []

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1400,900")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    # Try to find Chrome binary
    for path in ["/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser"]:
        if os.path.exists(path):
            options.binary_location = path
            break

    try:
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        print(f"[ParlayPlay] Chrome/ChromeDriver not available: {e}")
        return []

    all_players = []

    try:
        # Load homepage – this passes Cloudflare and triggers
        # the initial sport=All&period=FG API call automatically.
        driver.get("https://parlayplay.io")
        time.sleep(8)

        title = driver.title
        if "moment" in title.lower() or "challenge" in title.lower():
            time.sleep(10)
            title = driver.title

        if "ParlayPlay" not in title:
            print(f"[ParlayPlay] Page blocked by Cloudflare: {title}")
            return []

        # Extract API responses from performance logs
        logs = driver.get_log("performance")
        all_data = _plp_extract_api_data(driver, logs)

        # Parse all captured API data
        for url, data in all_data.items():
            if isinstance(data, dict) and "players" in data:
                for p in data["players"]:
                    entries = _plp_normalize_players(p)
                    all_players.extend(entries)

        # Deduplicate
        seen = set()
        deduped = []
        for p in all_players:
            key = f"{p['game']}||{p['player']}||{p['stat']}||{p['line']}"
            if key not in seen:
                seen.add(key)
                deduped.append(p)

        print(f"[ParlayPlay] {len(all_players)} raw, {len(deduped)} deduped lines")
        return deduped

    except Exception as e:
        print(f"[ParlayPlay] Error during scraping: {e}")
        return []
    finally:
        try:
            driver.quit()
        except Exception:
            pass


# ---------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------
def extract_map_and_stat(stat_str):
    """
    Extract the map portion and core stat from a stat string.

    Examples:
        "Kills on Maps 1+2"    -> ("Map 1+2", "Kills")
        "MAPS 1-2 Kills"       -> ("Map 1+2", "Kills")
        "Kills on Maps 1+2+3"  -> ("Map 1+2+3", "Kills")
        "MAP 1 Kills"          -> ("Map 1", "Kills")
        "Kills"                -> ("", "Kills")
    """
    s = stat_str.strip()

    # Pattern: "Stat on Maps X" (Underdog style)
    m = re.match(r"(.+?)\s+(?:on|in)\s+(maps?\s*[\d\+\-]+)", s, re.IGNORECASE)
    if m:
        core_stat = m.group(1).strip()
        map_part = m.group(2).strip()
    else:
        # Pattern: "Maps X Stat" or "MAP X Stat" (PrizePicks style)
        m = re.match(r"(maps?\s*[\d\+\-]+)\s+(.+)", s, re.IGNORECASE)
        if m:
            map_part = m.group(1).strip()
            core_stat = m.group(2).strip()
        else:
            return "", s

    # Normalize map display to use + notation
    ml = map_part.lower()
    if re.search(r"1[\+\-]2[\+\-]3|1\s*-\s*3", ml):
        map_display = "Map 1+2+3"
    elif re.search(r"1[\+\-]2|1\s*-\s*2", ml):
        map_display = "Map 1+2"
    elif re.search(r"(?:^|\s)1(?!\d)", ml):
        map_display = "Map 1"
    elif re.search(r"(?:^|\s)2(?!\d)", ml):
        map_display = "Map 2"
    elif re.search(r"(?:^|\s)3(?!\d)", ml):
        map_display = "Map 3"
    else:
        map_display = map_part

    return map_display, core_stat


def parse_scheduled_time(scheduled):
    """Parse a scheduled time string into a timezone-aware datetime, or None."""
    if not scheduled:
        return None
    try:
        return datetime.fromisoformat(scheduled.replace("Z", "+00:00"))
    except Exception:
        return None


def format_event(game, match, scheduled):
    """
    Format event string: "LoL - FlyQuest vs. LYON - 2/21/2026, 2:00 PM MST"
    """
    game_label = GAME_LABELS.get(game, game)

    date_str = ""
    if scheduled:
        try:
            dt = datetime.fromisoformat(scheduled.replace("Z", "+00:00"))
            # Convert to MST (UTC-7)
            dt_mst = dt - timedelta(hours=7)
            # Format: M/DD/YYYY, H:MM PM MST
            hour = dt_mst.hour % 12 or 12
            ampm = "AM" if dt_mst.hour < 12 else "PM"
            date_str = f"{dt_mst.month}/{dt_mst.day}/{dt_mst.year}, {hour}:{dt_mst.minute:02d} {ampm} MST"
        except Exception:
            date_str = scheduled

    parts = [game_label]
    if match:
        parts.append(match)
    if date_str:
        parts.append(date_str)
    return " - ".join(parts)


def format_line_cell(line_val, higher=None, lower=None):
    """
    Format a line value with odds for display in a cell.
    Examples: "13.5 (-128/+104)", "14", "6.5 (-115)"
    """
    if line_val is None or line_val == "":
        return ""

    # Format the line value (drop trailing .0)
    if isinstance(line_val, float) and line_val == int(line_val):
        val_str = str(int(line_val))
    else:
        val_str = str(line_val)

    # Add odds if available
    if higher and lower:
        return f"{val_str} ({higher}/{lower})"
    elif higher:
        return f"{val_str} ({higher})"
    elif lower:
        return f"{val_str} ({lower})"
    return val_str


# ---------------------------------------------------------------
# Normalization for cross-platform matching
# ---------------------------------------------------------------
def normalize_name(name):
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]", "", (name or "").lower().strip()))


def normalize_map_range(stat):
    s = (stat or "").lower()
    if re.search(r"maps?\s*1[\+\-]2[\+\-]3", s) or re.search(r"maps?\s*1\s*-\s*3", s):
        return "m123"
    if re.search(r"maps?\s*1[\+\-]2", s) or re.search(r"maps?\s*1\s*-\s*2", s):
        return "m12"
    if re.search(r"map\s*1(?!\d)", s):
        return "m1"
    if re.search(r"map\s*2(?!\d)", s):
        return "m2"
    if re.search(r"map\s*3(?!\d)", s):
        return "m3"
    return ""


def normalize_stat_for_matching(stat):
    s = (stat or "").lower().strip()
    map_range = normalize_map_range(s)
    s = re.sub(r"\s*(?:on|in)\s+maps?\s+[\d\+\-]+", "", s)
    s = re.sub(r"maps?\s+[\d\-\+]+\s*", "", s)
    s = re.sub(r"\s*\(.*?\)", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return f"{s} {map_range}" if map_range else s


# ---------------------------------------------------------------
# Build rows in the target format
# ---------------------------------------------------------------
def format_multiplier_cell(line_val, over_mult=None, under_mult=None):
    """
    Format a ParlayPlay line value with multipliers for display.
    ParlayPlay uses payout multipliers (e.g., 1.5x) instead of American odds.
    Examples: "3.5 (1.5x/1.8x)", "14 (1.2x)"
    """
    if line_val is None or line_val == "":
        return ""

    if isinstance(line_val, float) and line_val == int(line_val):
        val_str = str(int(line_val))
    else:
        val_str = str(line_val)

    over_str = f"{over_mult}x" if over_mult else ""
    under_str = f"{under_mult}x" if under_mult else ""

    if over_str and under_str:
        return f"{val_str} ({over_str}/{under_str})"
    elif over_str:
        return f"{val_str} ({over_str})"
    elif under_str:
        return f"{val_str} ({under_str})"
    return val_str


def build_sheet_rows(ud_lines, pp_lines, plp_lines=None):
    """
    Build rows in the format:
    Event | Player | Map | Stat | PrizePicks | PrizePicks Opening |
    Underdog | Underdog Opening | Betr | ParlayPlay | Dabble | Bovada | MyBookie

    Each unique Event+Player+Map+Stat gets one row.
    All platforms' values go on the same row when they match.
    Rows for matches that have already started are excluded.
    """
    now = datetime.now(timezone.utc)
    all_lines = ud_lines + pp_lines + (plp_lines or [])
    rows_by_key = {}
    skipped_started = 0

    for line in all_lines:
        # Skip lines for matches that have already started
        scheduled_dt = parse_scheduled_time(line.get("scheduled", ""))
        if scheduled_dt and scheduled_dt <= now:
            skipped_started += 1
            continue

        map_display, core_stat = extract_map_and_stat(line.get("stat", ""))
        event = format_event(line.get("game", ""), line.get("match", ""), line.get("scheduled", ""))
        norm_key = f"{normalize_name(line.get('player', ''))}||{normalize_stat_for_matching(line.get('stat', ''))}||{line.get('game', '')}"

        if norm_key not in rows_by_key:
            rows_by_key[norm_key] = {
                "event": event,
                "player": line.get("player", ""),
                "map": map_display,
                "stat": core_stat,
                "prizepicks": "",
                "underdog": "",
                "parlayplay": "",
                "game": line.get("game", ""),
            }

        row = rows_by_key[norm_key]

        if line.get("platform") == "Underdog":
            row["underdog"] = format_line_cell(
                line.get("line", ""),
                higher=line.get("higher_price"),
                lower=line.get("lower_price"),
            )
        elif line.get("platform") == "PrizePicks":
            row["prizepicks"] = format_line_cell(line.get("line", ""))
        elif line.get("platform") == "ParlayPlay":
            row["parlayplay"] = format_multiplier_cell(
                line.get("line", ""),
                over_mult=line.get("over_multiplier"),
                under_mult=line.get("under_multiplier"),
            )

    if skipped_started:
        print(f"[Filter] Skipped {skipped_started} lines for matches already started")

    return sorted(
        rows_by_key.values(),
        key=lambda r: (r["event"], r["player"], r.get("map", ""), r["stat"]),
    )


# ---------------------------------------------------------------
# Google Sheets clearing
# ---------------------------------------------------------------
def clear_sheet_data():
    """
    Clear all data rows (except the header) from the Google Sheet
    using the Sheets API and the hardcoded service account.
    """
    try:
        from google.oauth2.service_account import Credentials
        from google.auth.transport.requests import Request as AuthRequest
    except ImportError:
        try:
            import subprocess
            subprocess.check_call(
                ["pip", "install", "google-auth"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            from google.oauth2.service_account import Credentials
            from google.auth.transport.requests import Request as AuthRequest
        except Exception as e:
            print(f"[Clear] Could not install google-auth: {e}")
            return False

    try:
        creds = Credentials.from_service_account_info(
            SERVICE_ACCOUNT_INFO,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        creds.refresh(AuthRequest())

        # Clear everything below the header row
        range_to_clear = f"{SHEET_NAME}!A2:Z"
        url = (
            f"https://sheets.googleapis.com/v4/spreadsheets/"
            f"{SPREADSHEET_ID}/values/{range_to_clear}:clear"
        )
        resp = requests.post(
            url,
            headers={"Authorization": f"Bearer {creds.token}",
                     "Content-Type": "application/json"},
            json={},
        )
        if resp.status_code == 200:
            print("[Clear] Cleared existing data rows from sheet")
            return True
        else:
            print(f"[Clear] Error {resp.status_code}: {resp.text}")
            return False
    except Exception as e:
        print(f"[Clear] Error clearing sheet: {e}")
        return False


# ---------------------------------------------------------------
# Opening line tracking
# ---------------------------------------------------------------
def merge_with_opening_lines(new_rows, existing_data):
    """
    Merge new live data with existing opening line data.

    For each row:
    - If the row existed before, ALWAYS keep the original opening value
      (the oldest known value from the very first time the row appeared).
    - If the row is new, set opening = current live value.
    """
    existing_lookup = {}

    if existing_data:
        events = existing_data.get("event", [])
        players = existing_data.get("player", [])
        maps = existing_data.get("map", [])
        stats = existing_data.get("stat", [])
        pp_openings = existing_data.get("pp_opening", [])
        ud_openings = existing_data.get("ud_opening", [])

        row_count = max(
            len(events),
            len(players),
            len(maps),
            len(stats),
            len(pp_openings),
            len(ud_openings),
        )

        for i in range(row_count):
            key = (
                events[i] if i < len(events) else "",
                players[i] if i < len(players) else "",
                maps[i] if i < len(maps) else "",
                stats[i] if i < len(stats) else "",
            )
            existing_lookup[key] = {
                "pp_opening": pp_openings[i] if i < len(pp_openings) else "",
                "ud_opening": ud_openings[i] if i < len(ud_openings) else "",
            }

    for row in new_rows:
        key = (row["event"], row["player"], row["map"], row["stat"])
        existing = existing_lookup.get(key)

        if existing:
            # Always preserve the oldest known opening value
            row["prizepicks_opening"] = existing["pp_opening"] if existing["pp_opening"] else row["prizepicks"]
            row["underdog_opening"] = existing["ud_opening"] if existing["ud_opening"] else row["underdog"]
        else:
            # New row: opening = current live value
            row["prizepicks_opening"] = row["prizepicks"]
            row["underdog_opening"] = row["underdog"]

    return new_rows


# ---------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------

# Fetch live data from all platforms
ud_lines = fetch_underdog()
pp_lines = fetch_prizepicks()
plp_lines = fetch_parlayplay()

# Build rows (automatically filters out started matches)
new_rows = build_sheet_rows(ud_lines, pp_lines, plp_lines)

# Build existing data from inputs (from Google Sheets Reader node)
# These variables are provided by Gumloop when the node has inputs connected.
# If running standalone or on first run, they will be empty lists.
try:
    existing_data = {
        "event": existing_event if isinstance(existing_event, list) else [],
        "player": existing_player if isinstance(existing_player, list) else [],
        "map": existing_map if isinstance(existing_map, list) else [],
        "stat": existing_stat if isinstance(existing_stat, list) else [],
        "pp_opening": existing_pp_opening if isinstance(existing_pp_opening, list) else [],
        "ud_opening": existing_ud_opening if isinstance(existing_ud_opening, list) else [],
    }
except NameError:
    # First run or no existing data connected
    existing_data = None

# Merge with opening line data
final_rows = merge_with_opening_lines(new_rows, existing_data)

# Clear the sheet before the Writer appends new rows
clear_sheet_data()

print(f"\nTotal rows: {len(final_rows)}")
print(f"  With PrizePicks:  {sum(1 for r in final_rows if r['prizepicks'])}")
print(f"  With Underdog:    {sum(1 for r in final_rows if r['underdog'])}")
print(f"  With ParlayPlay:  {sum(1 for r in final_rows if r.get('parlayplay'))}")

# ---------------------------------------------------------------
# Output lists for Google Sheets Writer node
# ---------------------------------------------------------------
event = [r["event"] for r in final_rows]
player = [r["player"] for r in final_rows]
map_col = [r["map"] for r in final_rows]
stat = [r["stat"] for r in final_rows]
prizepicks = [r["prizepicks"] for r in final_rows]
prizepicks_opening = [r.get("prizepicks_opening", "") for r in final_rows]
underdog = [r["underdog"] for r in final_rows]
underdog_opening = [r.get("underdog_opening", "") for r in final_rows]
betr = [""] * len(final_rows)
parlayplay = [r.get("parlayplay", "") for r in final_rows]
dabble = [""] * len(final_rows)
bovada = [""] * len(final_rows)
mybookie = [""] * len(final_rows)
