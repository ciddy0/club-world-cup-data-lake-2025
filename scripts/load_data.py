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

    # Upsert teams (to avoid duplicates) (DENORMORALIZE THIS SHIT)
    for team in results["team_stats"]:
        supabase.table("teams").upsert({
            "team_id": team["teamId"],
            "team_name": team["teamName"],
            "abbreviation": team.get("abbreviation"),
            "logo": team.get("logo")
        }, on_conflict=["team_id"]).execute()

    # Insert team stats linked to team_id and match_id
    for team in results["team_stats"]:
        stats = team["stats"]
        supabase.table("team_stats").insert({
            "match_id": match_id,
            "team_id": team["teamId"],
            "fouls": int(stats.get("foulsCommitted", 0)),
            "yellow_cards": int(stats.get("yellowCards", 0)),
            "red_cards": int(stats.get("redCards", 0)),
            "offsides": int(stats.get("offsides", 0)),
            "corners": int(stats.get("wonCorners", 0)),
            "saves": int(stats.get("saves", 0))
        }).execute()

    # Insert player data
    for player in results["rosters"]:
        # Upsert player info
        supabase.table("players").upsert({
            "player_id": player["playerId"],
            "full_name": player["fullName"],
            "team_id": player["teamId"],
            "team_name": player["team"],
            "position": player.get("position"),
            "position_abbr": player.get("position_abbr"),
            "jersey": player["jersey"],
            "headshot": player.get("headshot")
        }, on_conflict=["player_id"]).execute()

        # Insert player stats
        supabase.table("player_stats").insert({
            "match_id": match_id,
            "player_id": player["playerId"],
            "starter": player["starter"],
            "active": player["active"],
            "subbed_in": player["subbedIn"],
            "subbed_out": player["subbedOut"],
            "goals": int(stats.get("totalGoals", 0) or 0),
            "assists": int(stats.get("goalAssists", 0) or 0),
            "shots": int(stats.get("totalShots", 0) or 0),
            "shots_on_target": int(stats.get("shotsOnTarget", 0) or 0),
            "yellow_cards": int(stats.get("yellowCards", 0) or 0),
            "red_cards": int(stats.get("redCards", 0) or 0),
            "fouls_committed": int(stats.get("foulsCommitted", 0) or 0),
            "fouls_suffered": int(stats.get("foulsSuffered", 0) or 0),
            "offsides": int(stats.get("offsides", 0) or 0),
            "stats": player["stats"]
        }).execute()