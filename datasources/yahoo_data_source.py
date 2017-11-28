# Data source code to access Yahoo! Fantasy Football data
#
# contributors: Joe M.
#
import collections
import os
import sys
import webbrowser
from badboystats import BadBoyStats
from yql3 import *
from yql3.storage import FileTokenStore


class YahooDataSource(object):
    """
    This class packages accessing and processing of data from Yahoo Fantasy Football API for the
    additional metrics and reports tool
    """

    def __init__(self, config):
        self.league_roster_active_slots = []
        self.roster = {}
        self.y3 = None
        self.config = config
        self.command_line_only = self.config.getboolean("OAuth_Settings", "command_line_only")
        self.token = None
        self.league_key = None
        self.league_id = 0
        self.teams_data = None
        self.league_standings_data = None
        self.league_name = ''
        self.BadBoy = BadBoyStats()

    def get_league_roster_active_slots(self):
        """ Get league roster active slots info
        """
        return self.league_roster_active_slots

    def get_league_standings_data(self):
        """ Get league standings data
        """
        return self.league_standings_data

    def get_league_name(self):
        """ Get league name
        """
        return self.league_name

    def __yql_query(self, query):
        """
        :param query: query string to hit Yahoo API
        :return: Rows results
        """
        print("Executing query: %s\n" % query)
        return self.y3.execute(query, token=self.token).rows

    def do_auth(self):
        """ Perform Yahoo authentication in preparation for querying API
        :return: Nothing
        """
        try:
            with open("./authentication/private.txt", "r") as auth_file:
                auth_data = auth_file.read().split("\n")
            consumer_key = auth_data[0]
            consumer_secret = auth_data[1]

            # yahoo oauth process
            self.y3 = ThreeLegged(consumer_key, consumer_secret)
            _cache_dir = self.config.get("OAuth_Settings", "yql_cache_dir")
            if not os.access(_cache_dir, os.R_OK):
                os.mkdir(_cache_dir)

            token_store = FileTokenStore(_cache_dir, secret="sasfasdfdasfdaf")
            stored_token = token_store.get("foo")

            if not stored_token:
                request_token, auth_url = self.y3.get_token_and_auth_url()

                if self.command_line_only:
                    print("Visit url %s and get a verifier string" % auth_url)
                else:
                    webbrowser.open(auth_url.decode('utf-8'))

                verifier = input("Enter the code: ")
                self.token = self.y3.get_access_token(request_token, verifier)
                token_store.set("foo", self.token)

            else:
                print("Verifying token...")
                self.token = self.y3.check_token(stored_token)
                if self.token != stored_token:
                    print("Setting stored token!")
                    token_store.set("foo", self.token)
            return True
        except Exception:
            print("Unexpected error:", sys.exc_info()[0])
            return False

    def get_league_key(self, league_id):
        """ get fantasy football game info
        :return: League id as a string in the form GAME_KEY.l.LEAGUE_ID
        """
        game_data = self.__yql_query("select * from fantasysports.games where game_key='nfl'")
        # unique league key composed of this year's yahoo fantasy football game id and the unique league id
        self.league_key = game_data[0].get("game_key") + ".l." + str(league_id)

    def get_league_team_info(self):
        """ get data for all teams in league
        :return: Nothing - populates class members with data for teams, league standings, and league name
        """
        self.teams_data = self.__yql_query("select * from fantasysports.teams where league_key='" +
                                           self.league_key + "'")

        # get data for all league standings
        self.league_standings_data = self.__yql_query(
            "select * from fantasysports.leagues.standings where league_key='" + self.league_key + "'")
        self.league_name = self.league_standings_data[0].get("name")

    def get_roster_settings(self):
        """ get individual league roster
        :return: Returns roster settings data structure
        """
        roster_data = self.__yql_query(
            "select * from fantasysports.leagues.settings where league_key='" + self.league_key + "'")

        roster_slots = collections.defaultdict(int)
        flex_positions = []

        print(self.league_key)
        for position in roster_data[0].get("settings").get("roster_positions").get("roster_position"):

            position_name = position.get("position")
            position_count = int(position.get("count"))

            count = position_count
            while count > 0:
                if position_name != "BN":
                    self.league_roster_active_slots.append(position_name)
                count -= 1

            if position_name == "W/R":
                flex_positions = ["WR", "RB"]
            if position_name == "W/R/T":
                flex_positions = ["WR", "RB", "TE"]

            if "/" in position_name:
                position_name = "FLEX"

            roster_slots[position_name] += position_count

        self.roster = {
            "slots": roster_slots,
            "flex_positions": flex_positions
        }
        return self.roster

    def retrieve_scoreboard(self, chosen_week):
        """
        get weekly matchup data for metrics calculations
        :param chosen_week: Week number for which to retrieve scoreboard data
        :return: Matchup list in the following format:

        [
            {
                'team1': {
                    'result': 'W',
                    'score': 100
                },
                'team2': {
                    'result': 'L',
                    'score': 50
                }
            },
            {
                'team3': {
                    'result': 'T',
                    'score': 75
                },
                'team4': {
                    'result': 'T',
                    'score': 75
                }
            }
        ]
        """
        result = self.__yql_query(
            "select * from fantasysports.leagues.scoreboard where league_key='{0}' and week='{1}'".format(
                self.league_key, chosen_week))

        matchups = result[0].get("scoreboard").get("matchups").get("matchup")

        matchup_list = []

        for matchup in matchups:

            if matchup.get("status") == "postevent":
                winning_team = matchup.get("winner_team_key")
                is_tied = int(matchup.get("is_tied"))
            elif matchup.get("status") == "midevent":
                winning_team = ""
                is_tied = 1
            else:
                winning_team = ""
                is_tied = 0

            def team_result(team):
                """
                determine if team tied/won/lost
                """
                team_key = team.get("team_key")

                if is_tied:
                    return "T"

                return "W" if team_key == winning_team else "L"

            teams = {
                team.get("name"): {
                    "result": team_result(team),
                    "score": team.get("team_points").get("total")
                } for team in matchup.get("teams").get("team")
            }

            matchup_list.append(teams)

        return matchup_list

    def retrieve_data(self, chosen_week):
        """ Builds and returns the team results dict for use in metrics calculations
        :param chosen_week: Return comprehensive data structure for given week
        :return: Comprehensive data structure with info to support metrics calculations
        """
        teams_dict = {}
        for team in self.teams_data:

            team_id = team.get("team_id")
            team_name = team.get("name")
            team_managers = team.get("managers").get("manager")

            team_manager = ""
            if type(team_managers) is dict:
                team_manager = team_managers.get("nickname")
            else:
                for manager in team_managers:
                    if manager.get("is_comanager") is None:
                        team_manager = manager.get("nickname")

            team_info_dict = {"name": team_name, "manager": team_manager}
            teams_dict[team_id] = team_info_dict

        team_results_dict = {}

        # iterate through all teams and build team_results_dict containing all relevant team stat information
        for team in teams_dict:

            team_id = team
            team_name = teams_dict.get(team).get("name").encode("utf-8")

            # get data for this individual team
            roster_stats_data = self.__yql_query(
                "select * from fantasysports.teams.roster.stats where team_key='" + self.league_key + ".t." +
                team + "' and week='" + chosen_week + "'")

            players = []
            positions_filled_active = []
            for player in roster_stats_data[0].get("roster").get("players").get("player"):
                pname = player.get("name")['full']
                pteam = player.get('editorial_team_abbr').upper()
                player_selected_position = player.get("selected_position").get("position")
                bad_boy_points = 0
                crime = ''
                if player_selected_position != "BN":
                    bad_boy_points, crime = self.BadBoy.check_bad_boy_status(pname, pteam, player_selected_position)
                    positions_filled_active.append(player_selected_position)

                player_info_dict = {"name": player.get("name")["full"],
                                    "status": player.get("status"),
                                    "bye_week": int(player.get("bye_weeks")["week"]),
                                    "selected_position": player.get("selected_position").get("position"),
                                    "eligible_positions": player.get("eligible_positions").get("position"),
                                    "fantasy_points": float(player.get("player_points").get("total", 0.0)),
                                    "bad_boy_points": bad_boy_points,
                                    "bad_boy_crime": crime
                                    }

                players.append(player_info_dict)

            team_name = team_name.decode('utf-8')
            bad_boy_total = 0
            worst_offense = ''
            worst_offense_score = 0
            num_offenders = 0
            for p in players:
                if p['selected_position'] != "BN":
                    bad_boy_total = bad_boy_total + p['bad_boy_points']
                    if p['bad_boy_points'] > 0:
                        num_offenders = num_offenders + 1
                        if p['bad_boy_points'] > worst_offense_score:
                            worst_offense = p['bad_boy_crime']
                            worst_offense_score = p['bad_boy_points']

            team_results_dict[team_name] = {
                "name": team_name,
                "manager": teams_dict.get(team).get("manager"),
                "players": players,
                "score": sum([p["fantasy_points"] for p in players if p["selected_position"] != "BN"]),
                "bench_score": sum([p["fantasy_points"] for p in players if p["selected_position"] == "BN"]),
                "team_id": team_id,
                "bad_boy_points": bad_boy_total,
                "worst_offense": worst_offense,
                "num_offenders": num_offenders,
                "positions_filled_active": positions_filled_active
            }

        return team_results_dict
