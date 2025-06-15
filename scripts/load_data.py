from supabase import create_client
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def load_to_supabase(results, match_id, match_info):
    # Insert match metadata
    supabase.table("matches").insert({
        "id": match_id,
        "date": match_info["date"],
        "home_team": match_info["home_team"],
        "away_team": match_info["away_team"],
        "home_team_id": match_info["home_team_id"],
        "away_team_id": match_info["away_team_id"]
    }).execute()

    # Insert team stats
    for team in results["team_stats"]:
        stats = team["stats"]
        supabase.table("team_stats").insert({
            "match_id": match_id,
            "team_id": team["teamId"],
            "team_name": team["teamName"],
            "abbreviation": team["abbreviation"],
            "fouls": int(stats.get("foulsCommitted", 0)),
            "yellow_cards": int(stats.get("yellowCards", 0)),
            "red_cards": int(stats.get("redCards", 0)),
            "offsides": int(stats.get("offsides", 0)),
            "corners": int(stats.get("wonCorners", 0)),
            "saves": int(stats.get("saves", 0))
        }).execute()

    # Insert player stats
    for player in results["rosters"]:
        supabase.table("player_stats").insert({
            "match_id": match_id,
            "team_id": player["teamId"],
            "team_name": player["team"],
            "player_id": player["playerId"],
            "full_name": player["fullName"],
            "jersey": player["jersey"],
            "starter": player["starter"],
            "active": player["active"],
            "subbed_in": player["subbedIn"],
            "subbed_out": player["subbedOut"],
            "stats": player["stats"]
        }).execute()