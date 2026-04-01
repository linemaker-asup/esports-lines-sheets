# Esports Lines Sheets

Fetches esports player prop lines from **PrizePicks** and **Underdog Fantasy**, then outputs them to a Google Sheet in a multi-book comparison format. Designed to run on [Gumloop](https://www.gumloop.com).

## Output Format

The Google Sheet is populated with the following columns:

| Column | Description |
|--------|-------------|
| **Event** | Game + match + date (e.g., `LoL - FlyQuest vs. LYON - 2/21/2026, 2:00 PM MST`) |
| **Player** | Player name |
| **Map** | Map scope: `Map 1`, `Map 2`, `Map 1+2`, `Map 1+2+3`, or empty |
| **Stat** | Core stat type: `Kills`, `Assists`, `Deaths`, etc. |
| **PrizePicks** | Live PrizePicks line (e.g., `14`) |
| **PrizePicks Opening** | The first line value seen for this player/stat (tracks line movement) |
| **Underdog** | Live Underdog line with odds (e.g., `13.5 (-128/+104)`) |
| **Underdog Opening** | The first Underdog line value seen (tracks line movement) |
| **Betr** | Empty (placeholder for future use) |
| **ParlayPlay** | Empty (placeholder for future use) |
| **Dabble** | Empty (placeholder for future use) |
| **Bovada** | Empty (placeholder for future use) |
| **MyBookie** | Empty (placeholder for future use) |

### Opening Line Tracking

The script tracks line movement by preserving the **first line value seen** for each player/stat:

- **First run**: Both the live and opening columns are set to the current value.
- **Subsequent runs**: The live column updates to the latest value, while the opening column retains the original value from the first run.

This lets you see at a glance how lines have moved since they opened.

## Data Sources

| Platform | Source | Notes |
|----------|--------|-------|
| **Underdog Fantasy** | Direct API (`api.underdogfantasy.com`) | Includes American odds (higher/lower) |
| **PrizePicks** | Via proxy (`pp-python.vercel.app`) | Proxy uses `curl_cffi` TLS impersonation to bypass PerimeterX |

The PrizePicks proxy is deployed separately (see the [esports-lines-dashboard](https://github.com/linemaker-asup/esports-lines-dashboard) repo for the proxy code in `pp-python/`).

## Supported Games

| Code | Display Label |
|------|---------------|
| `LOL` | LoL |
| `CS2` | CS2 |
| `DOTA2` | Dota 2 |
| `VAL` | Valorant |
| `COD` | Call of Duty |

## Gumloop Setup

### Step 1: Create the Google Sheet

1. Create a new Google Sheet.
2. Add these headers in the first row (exactly as shown):

```
Event | Player | Map | Stat | PrizePicks | PrizePicks Opening | Underdog | Underdog Opening | Betr | ParlayPlay | Dabble | Bovada | MyBookie
```

3. Name the sheet tab (at the bottom) something like `Lines`.

### Step 2: Build the Gumloop Workflow

The workflow has 4 nodes:

```
Schedule Trigger → Google Sheets Reader → Run Code (Python) → Google Sheets Writer
```

#### Node 1: Schedule Trigger
- Set the frequency (e.g., every 30 minutes, every hour).

#### Node 2: Google Sheets Reader
- Connect to your Google Sheet.
- Set **Sheet Name** to your tab name (e.g., `Lines`).
- This reads existing data so opening lines are preserved.

#### Node 3: Run Code (Python)

1. Copy the entire contents of `gumloop_script.py` into the **Function Body**.

2. Set these **Inputs** (connect from the Google Sheets Reader outputs):

| Input Name | Connect From (Reader Column) |
|---|---|
| `existing_event` | Event |
| `existing_player` | Player |
| `existing_map` | Map |
| `existing_stat` | Stat |
| `existing_pp` | PrizePicks |
| `existing_pp_opening` | PrizePicks Opening |
| `existing_ud` | Underdog |
| `existing_ud_opening` | Underdog Opening |

3. Set these **Outputs**:

```
event, player, map_col, stat, prizepicks, prizepicks_opening, underdog, underdog_opening, betr, parlayplay, dabble, bovada, mybookie
```

#### Node 4: Google Sheets Writer
- Connect to the same Google Sheet.
- Set **Writer Mode** to **"Add New Rows"**.
- Set **Sheet Name** to your tab name.
- Map the Run Code outputs to sheet columns:

| Sheet Column | Connect to Output |
|---|---|
| Event | `event` |
| Player | `player` |
| Map | `map_col` |
| Stat | `stat` |
| PrizePicks | `prizepicks` |
| PrizePicks Opening | `prizepicks_opening` |
| Underdog | `underdog` |
| Underdog Opening | `underdog_opening` |
| Betr | `betr` |
| ParlayPlay | `parlayplay` |
| Dabble | `dabble` |
| Bovada | `bovada` |
| MyBookie | `mybookie` |

### Step 3: Handle Sheet Clearing (Important)

Since the workflow replaces all data each run, you need to **clear existing rows before writing**. Options:

1. **Add a Google Sheets MCP node** before the Writer that clears all rows except the header.
2. **Or** add a small Run Code node before the Writer that calls the Google Sheets API to clear the sheet.
3. **Or** use a Google Sheets Updater in "upsert" mode keyed on Event+Player+Map+Stat (more complex but avoids clearing).

The simplest approach is option 1: use a Google Sheets MCP node with the prompt "Clear all rows except the header row in sheet [your sheet name]".

### Step 4: Enable the Workflow

1. Save the workflow.
2. Click **Run** to test it once.
3. Enable the schedule trigger for automatic runs.

## Stat Normalization

Lines are matched across platforms using normalized player names and stat types. Map ranges from different platforms are unified:

| Platform | Raw Stat | Map | Core Stat |
|----------|----------|-----|-----------|
| Underdog | `Kills on Maps 1+2` | Map 1+2 | Kills |
| PrizePicks | `MAPS 1-2 Kills` | Map 1+2 | Kills |
| Underdog | `Kills on Maps 1+2+3` | Map 1+2+3 | Kills |
| PrizePicks | `MAPS 1-3 Kills` | Map 1+2+3 | Kills |
| Underdog | `Map 1 Assists` | Map 1 | Assists |

## Running Locally (Optional)

You can test the script locally without Gumloop:

```bash
pip install requests
python gumloop_script.py
```

This will fetch live data and print a summary. The Google Sheets outputs won't be written (they're consumed by Gumloop nodes), but you can verify the data is being fetched and formatted correctly.
