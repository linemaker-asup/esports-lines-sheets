"""
Gumloop Run Code Node: Esports Lines to Google Sheets

Fetches esports lines from Underdog Fantasy and PrizePicks,
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
from datetime import datetime, timezone, timedelta


# ---------------------------------------------------------------
# Config
# ---------------------------------------------------------------
UNDERDOG_URL = "https://api.underdogfantasy.com/beta/v5/over_under_lines"
PP_PROXY_URL = "https://pp-python.vercel.app/api/prizepicks"
TARGET_SPORTS = {"LOL", "CS", "DOTA2", "ESPORTS"}

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
def build_sheet_rows(ud_lines, pp_lines):
    """
    Build rows in the format:
    Event | Player | Map | Stat | PrizePicks | PrizePicks Opening |
    Underdog | Underdog Opening | Betr | ParlayPlay | Dabble | Bovada | MyBookie

    Each unique Event+Player+Map+Stat gets one row.
    Both platforms' values go on the same row when they match.
    Rows for matches that have already started are excluded.
    """
    now = datetime.now(timezone.utc)
    all_lines = ud_lines + pp_lines
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

    if skipped_started:
        print(f"[Filter] Skipped {skipped_started} lines for matches already started")

    return sorted(
        rows_by_key.values(),
        key=lambda r: (r["event"], r["player"], r.get("map", ""), r["stat"]),
    )


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

# Fetch live data from both platforms
ud_lines = fetch_underdog()
pp_lines = fetch_prizepicks()

# Build rows (automatically filters out started matches)
new_rows = build_sheet_rows(ud_lines, pp_lines)

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

print(f"\nTotal rows: {len(final_rows)}")
print(f"  With PrizePicks: {sum(1 for r in final_rows if r['prizepicks'])}")
print(f"  With Underdog:   {sum(1 for r in final_rows if r['underdog'])}")

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
parlayplay = [""] * len(final_rows)
dabble = [""] * len(final_rows)
bovada = [""] * len(final_rows)
mybookie = [""] * len(final_rows)
