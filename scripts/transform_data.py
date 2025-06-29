import os
import json
from datetime import datetime
from load_data import load_to_supabase

def extract_summary_data_from_files(event_ids, raw_data_dir="/opt/airflow/data/raw"):
    all_match_data = []

    for event_id in event_ids:
        # Find the saved file for this event_id
        matching_files = [
            f for f in os.listdir(raw_data_dir)
            if f.startswith(f"summary_{event_id}_") and f.endswith(".json")
        ]
        if not matching_files:
            print(f"No summary file found for event {event_id}")
            continue
        
        # Pick latest file by name (which includes timestamp)
        matching_files.sort(reverse=True)
        file_path = os.path.join(raw_data_dir, matching_files[0])

        with open(file_path, "r") as f:
            data = json.load(f)

        boxscore_form = data.get("boxscore", {}).get("form", [])
        if len(boxscore_form) < 2:
            print(f"Boxscore form incomplete for event {event_id}")
            continue

        home_team = boxscore_form[0].get("team")
        away_team = boxscore_form[1].get("team")

        if not home_team or not away_team:
            print(f"Missing team info for event {event_id}")
            continue

        game_date = (
            data.get("game", {}).get("date") or
            data.get("header", {}).get("competitions", [{}])[0].get("date")
        )

        match_info = {
            "date": game_date,
            "home_team": home_team.get("displayName"),
            "away_team": away_team.get("displayName"),
            "home_team_id": home_team.get("id"),
            "away_team_id": away_team.get("id")
        }

        results = {
            "rosters": [],
            "team_stats": []
        }

        # Team stats extraction
        for team_entry in data.get("boxscore", {}).get("teams", []):
            team = team_entry.get("team", {})
            stats = team_entry.get("statistics", [])
            stats_dict = {
                s.get("name"): s.get("value") or s.get("displayValue") 
                for s in stats
            }

            results["team_stats"].append({
                "teamId": team.get("id"),
                "teamName": team.get("displayName"),
                "abbreviation": team.get("abbreviation"),
                "logo": team.get("logo"),
                "stats": stats_dict
            })

        # Player stats extraction
        for team_roster in data.get("rosters", []):
            team_info = team_roster.get("team", {})
            home_away = team_roster.get("homeAway")
            players = team_roster.get("roster", [])

            for player in players:
                athlete = player.get("athlete", {})
                stats = player.get("stats", [])
                stats_dict = {
                    stat.get("name"): stat.get("value") or stat.get("displayValue") 
                    for stat in stats
                }
                results["rosters"].append({
                    "team": team_info.get("displayName"),
                    "teamId": team_info.get("id"),
                    "homeAway": home_away,
                    "playerId": athlete.get("id"),
                    "fullName": athlete.get("fullName"),
                    "jersey": player.get("jersey"),
                    "starter": player.get("starter"),
                    "active": player.get("active"),
                    "subbedIn": player.get("subbedIn"),
                    "subbedOut": player.get("subbedOut"),
                    "stats": stats_dict,
                    "position": player.get("position", {}).get("displayName"),
                    "position_abbr": player.get("position", {}).get("abbreviation"),
                    "headshot": athlete.get("headshot", {}).get("href")
                })

        all_match_data.append({
            "match_id": event_id,
            "match_info": match_info,
            "results": results
        })

    return all_match_data

def load_all_matches_to_supabase(**context):
    event_ids = context['ti'].xcom_pull(key="event_ids", task_ids="fetch_match_data", include_prior_dates=False)
    if not event_ids:
        print("No event IDs to process")
        return

    matches_data = extract_summary_data_from_files(event_ids)

    for match_data in matches_data:
        load_to_supabase(
            results=match_data["results"],
            match_id=match_data["match_id"],
            match_info=match_data["match_info"]
        )
