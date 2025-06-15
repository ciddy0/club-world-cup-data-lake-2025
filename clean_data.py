import json
import pandas as pd

# Load JSON data we got from the ESPN api endpoint
with open("data/raw/matches_20250602T230040Z.json") as f:
    data = json.load(f)

team_rows = []
match_events = []
# Iterate through all events
for event in data.get("events", []):
    match_id = event["id"]
    competition = event.get("competitions", [])[0]

    for comp in competition.get("competitors", []):
        team_info = comp.get("team", {})
        stats_list = comp.get("statistics", [])
        
        row = {
            "match_id": match_id,
            "team_id": team_info.get("id"),
            "team_name": team_info.get("displayName"),
            "home_away": comp.get("homeAway"),
            "score": comp.get("score"),
        }
        
        # Flatten each stat by name
        for stat in stats_list:
            key = stat.get("name")
            val = stat.get("displayValue")
            if key:
                row[key] = val
        
        team_rows.append(row)
    for dets in competition.get("details", []):
        clock_val = dets.get("clock", {}).get("value", 0)
        minute = int(clock_val / 60) if clock_val is not None else None
        team_id = dets.get("team", {}).get("id")

        for athlete in dets.get("athletesInvolved", []):
            match_events.append({
                "match_id": match_id,
                "player_id": athlete.get("id"),
                "player_name": athlete.get("fullName"),
                "team_id": team_id,
                "event_type": dets.get("type", {}).get("text"),
                "minute": minute,
                "is_goal": dets.get("scoringPlay", False),
                "is_card": dets.get("yellowCard", False) or dets.get("redCard", False),
                "is_yellow_card": dets.get("yellowCard", False),
                "is_red_card": dets.get("redCard", False),
                "is_penalty": dets.get("penaltyKick", False),  
                "is_own_goal": dets.get("ownGoal", False),
                "is_shootout": dets.get("shootout", False)
            })

# Create DataFrame
df_teams = pd.DataFrame(team_rows)
df_events = pd.DataFrame(match_events)

# Cleaning the data :D
df_teams.columns = df_teams.columns.str.lower().str.replace(" ", "_")
df_events.columns = df_events.columns.str.lower().str.replace(" ", "_")

# Convert score and minutes to integer
df_teams["score"] = pd.to_numeric(df_teams["score"], errors="coerce")
df_events["minute"] = pd.to_numeric(df_events["minute"], errors="coerce")

# Optional: Clean percentage columns
for col in df_teams.columns:
    if df_teams[col].dtype == "object" and df_teams[col].str.contains("%", na=False).any():
        df_teams[col] = df_teams[col].str.replace("%", "").astype(float)
for col in df_events.columns:
    if df_events[col].dtype == "object" and df_events[col].str.contains("%", na=False).any():
        df_events[col] = df_events[col].str.replace("%", "").astype(float)

# Fill missing values :o
df_teams.fillna(0, inplace=True)
df_events.fillna(0, inplace=True)

# turn into csv!!!
df_teams.to_csv("team_match_stats.csv", index=False)
df_events.to_csv("player_match_stats.csv", index=False)
