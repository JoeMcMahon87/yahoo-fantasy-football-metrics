# written by Wren J.R.
# contributors: Kevin N., Joe M.
# code snippets taken from: http://tech.thejoestory.com/2014/12/yahoo-fantasy-football-api-using-python.html

import collections
import datetime
import itertools
import os
from configparser import ConfigParser

from metrics import PointsByPosition, SeasonAverageCalculator, Breakdown, CalculateMetrics, PowerRanking
from badboystats import BadBoyStats
from pdf_generator import PdfGenerator
from datasources import yahoo_data_source


# noinspection SqlNoDataSourceInspection,SqlDialectInspection
class FantasyFootballReport(object):
    def __init__(self, user_input_league_id=None, user_input_chosen_week=None, test_bool=False):
        # config vars
        self.config = ConfigParser()
        self.config.read("config.ini")
        if user_input_league_id:
            self.league_id = user_input_league_id
        else:
            self.league_id = self.config.get("Fantasy_Football_Report_Settings", "chosen_league_id")

        self.test_bool = False
        if test_bool:
            self.test_bool = True

        # verification output message
        print("\nGenerating%s fantasy football report for league with id: %s (report generated: %s)\n" % (
            " TEST" if test_bool else "", self.league_id, "{:%Y-%b-%d %H:%M:%S}".format(datetime.datetime.now())))

        self.datasource = yahoo_data_source.YahooDataSource(self.config)
        if self.datasource.do_auth():
            self.BadBoy = BadBoyStats()

            self.league_key = self.datasource.get_league_key(self.league_id)
            league_name = self.datasource.get_league_name()
            self.roster = self.datasource.get_roster_settings()
            self.datasource.get_league_team_info()

            # user input validation
            if user_input_chosen_week:
                chosen_week = user_input_chosen_week
            else:
                chosen_week = self.config.get("Fantasy_Football_Report_Settings", "chosen_week")
            try:
                league_standings_data = self.datasource.get_league_standings_data()

                if chosen_week == 'default':
                    self.chosen_week = str(int(league_standings_data[0].get("current_week")) - 1)
                elif 0 < int(chosen_week) < 18:
                    if 0 < int(chosen_week) <= int(league_standings_data[0].get("current_week")) - 1:
                        self.chosen_week = chosen_week
                    else:
                        incomplete_week = input(
                            "Are you sure you want to generate a report for an incomplete week? (y/n) -> ")
                        if incomplete_week == "y":
                            self.chosen_week = chosen_week
                        elif incomplete_week == "n":
                            raise ValueError("It is recommended that you not generate a report for an incomplete week.")
                        else:
                            raise ValueError("Please only select 'y' or 'n'. Try running the report generator again.")
                else:
                    raise ValueError("You must select either 'default' or an integer from 1 to 17 for the chosen week.")
            except ValueError:
                raise ValueError("You must select either 'default' or an integer from 1 to 17 for the chosen week.")

            # output league info for verification
            print("\nGenerating \"{}\" ({}) report for week {} (chosen week: {}).\n".format(league_name.upper(),
                                                                                            self.league_key,
                                                                                            self.chosen_week,
                                                                                            chosen_week))

    def calculate_metrics(self, chosen_week):

        matchups_list = self.datasource.retrieve_scoreboard(chosen_week)
        team_results_dict = self.datasource.retrieve_data(chosen_week)

        # get current standings
        calculate_metrics = CalculateMetrics(self.league_id, self.config)

        # calculate coaching efficiency metric and add values to team_results_dict, and get and points by position
        points_by_position = PointsByPosition(self.roster, self.chosen_week)
        weekly_points_by_position_data = \
            points_by_position.get_weekly_points_by_position(self.league_id, self.config, chosen_week, self.roster,
                                                             self.datasource.get_league_roster_active_slots(),
                                                             team_results_dict)

        # calculate luck metric and add values to team_results_dict
        Breakdown().execute_breakdown(team_results_dict, matchups_list)

        power_ranking_metric = PowerRanking()
        power_ranking_results = power_ranking_metric.execute_power_ranking(team_results_dict)

        for team_name in team_results_dict:
            team_results_dict[team_name]["power_rank"] = power_ranking_results[team_name]["power_rank"]

        # used only for testing what happens when different metrics are tied; requires uncommenting lines in method
        if self.test_bool:
            calculate_metrics.test_ties(team_results_dict)

        for key in team_results_dict:
            try:
                st = team_results_dict[key].decode('utf-8')
                team_results_dict[key] = st
            except AttributeError:
                pass

        power_ranking_results = sorted(iter(list(team_results_dict.items())), key=lambda k_v: k_v[1]["power_rank"])
        power_ranking_results_data = []
        for key, value in power_ranking_results:
            # season avg calc does something where it keys off the second value in the array
            # WREN WHYYYYY
            power_ranking_results_data.append(
                [value.get("power_rank"), key, team_results_dict[key]["manager"]]
            )

        # create bad boy data for table
        bad_boy_results = sorted(iter(team_results_dict.items()),
                                 key=lambda k_v: (float(k_v[1].get("bad_boy_points")), k_v[0]), reverse=True)
        bad_boy_results_data = calculate_metrics.get_bad_boy_data(bad_boy_results)

        num_tied_bad_boys = calculate_metrics.get_num_ties(bad_boy_results_data, chosen_week, tie_type="bad_boy")

        tie_for_first_bad_boy = False
        if num_tied_bad_boys > 0:
            if bad_boy_results_data[0][0] == bad_boy_results_data[1][0]:
                tie_for_first_bad_boy = True
        num_tied_for_first_bad_boy = len(
            [list(group) for key, group in itertools.groupby(bad_boy_results_data, lambda x: x[3])][0])

        # create score data for table
        score_results = sorted(iter(team_results_dict.items()),
                               key=lambda k_v: (float(k_v[1].get("score")), k_v[0]), reverse=True)
        score_results_data = calculate_metrics.get_score_data(score_results)

        current_standings_data = calculate_metrics.get_standings(self.datasource.get_league_standings_data())

        # create coaching efficiency data for table
        coaching_efficiency_results = sorted(iter(team_results_dict.items()),
                                             key=lambda k_v1: (k_v1[1].get("coaching_efficiency"), k_v1[0]),
                                             reverse=True)
        coaching_efficiency_results_data = calculate_metrics.get_coaching_efficiency_data(
            coaching_efficiency_results)
        efficiency_dq_count = calculate_metrics.coaching_efficiency_dq_count

        # create luck data for table
        luck_results = sorted(iter(team_results_dict.items()),
                              key=lambda k_v2: (k_v2[1].get("luck"), k_v2[0]), reverse=True)
        luck_results_data = calculate_metrics.get_luck_data(luck_results)

        # count number of ties for points, coaching efficiency, and luck
        # tie_type can be "score", "coaching_efficiency", "luck", "bad_boy", or "power_rank"
        num_tied_scores = calculate_metrics.get_num_ties(score_results_data, chosen_week,
                                                         tie_type="score")
        # reorder score data based on bench points
        if num_tied_scores > 0:
            score_results_data = calculate_metrics.resolve_score_ties(score_results_data)
            calculate_metrics.get_num_ties(score_results_data, chosen_week, tie_type="score")
        tie_for_first_score = False
        if score_results_data[0][0] == score_results_data[1][0]:
            tie_for_first_score = True
        num_tied_for_first_scores = len(
            [list(group) for key, group in itertools.groupby(score_results_data, lambda x: x[3])][0])

        num_tied_coaching_efficiencies = calculate_metrics.get_num_ties(coaching_efficiency_results_data, chosen_week,
                                                                        tie_type="coaching_efficiency")
        tie_for_first_coaching_efficiency = False
        if coaching_efficiency_results_data[0][0] == coaching_efficiency_results_data[1][0]:
            tie_for_first_coaching_efficiency = True
        num_tied_for_first_coaching_efficiency = len(
            [list(group) for key, group in itertools.groupby(coaching_efficiency_results_data, lambda x: x[3])][0])

        num_tied_lucks = calculate_metrics.get_num_ties(luck_results_data, chosen_week, tie_type="luck")
        tie_for_first_luck = False
        if luck_results_data[0][0] == luck_results_data[1][0]:
            tie_for_first_luck = True
        num_tied_for_first_luck = len(
            [list(group) for key, group in itertools.groupby(luck_results_data, lambda x: x[3])][0])

        num_tied_power_rankings = calculate_metrics.get_num_ties(power_ranking_results_data,
                                                                 chosen_week, tie_type="power_rank")
        tie_for_first_power_ranking = False
        if power_ranking_results_data[0][0] == power_ranking_results_data[1][0]:
            tie_for_first_power_ranking = True
        num_tied_for_first_power_ranking = len(
            [list(group) for key, group in itertools.groupby(power_ranking_results_data, lambda x: x[0])][0])

        # output weekly metrics info
        print("~~~~~ WEEK {} METRICS INFO ~~~~~".format(chosen_week))
        print("              SCORE tie(s): {}".format(num_tied_scores))
        print("COACHING EFFICIENCY tie(s): {}".format(num_tied_coaching_efficiencies))
        print("               LUCK tie(s): {}".format(num_tied_lucks))
        print("      POWER RANKING tie(s): {}".format(num_tied_power_rankings))
        print("      BAD BOY SCORE tie(s): {}".format(num_tied_bad_boys))
        coaching_efficiency_dq_dict = points_by_position.coaching_efficiency_dq_dict
        if coaching_efficiency_dq_dict:
            ce_dq_str = ""
            for team in list(coaching_efficiency_dq_dict.keys()):
                if coaching_efficiency_dq_dict.get(team) == -1:
                    ce_dq_str += "{} (incomplete active squad), ".format(team)
                else:
                    ce_dq_str += "{} (ineligible bench players: {}/{}), ".format(team,
                                                                                 coaching_efficiency_dq_dict.get(team),
                                                                                 self.roster.get("slots").get("BN"))
            print("   COACHING EFFICIENCY DQs: {}\n".format(ce_dq_str[:-2]))
        else:
            print("")

        report_info_dict = {
            "team_results": team_results_dict,
            "current_standings_data": current_standings_data,
            "score_results_data": score_results_data,
            "coaching_efficiency_results_data": coaching_efficiency_results_data,
            "luck_results_data": luck_results_data,
            "power_ranking_results_data": power_ranking_results_data,
            "num_tied_scores": num_tied_scores,
            "num_tied_coaching_efficiencies": num_tied_coaching_efficiencies,
            "num_tied_lucks": num_tied_lucks,
            "num_tied_power_rankings": num_tied_power_rankings,
            "efficiency_dq_count": efficiency_dq_count,
            "tied_scores_bool": num_tied_scores > 0,
            "tied_coaching_efficiencies_bool": num_tied_coaching_efficiencies > 0,
            "tied_lucks_bool": num_tied_lucks > 0,
            "tied_power_rankings_bool": num_tied_power_rankings > 0,
            "tie_for_first_score": tie_for_first_score,
            "tie_for_first_coaching_efficiency": tie_for_first_coaching_efficiency,
            "tie_for_first_luck": tie_for_first_luck,
            "tie_for_first_power_ranking": tie_for_first_power_ranking,
            "num_tied_for_first_scores": num_tied_for_first_scores,
            "num_tied_for_first_coaching_efficiency": num_tied_for_first_coaching_efficiency,
            "num_tied_for_first_luck": num_tied_for_first_luck,
            "num_tied_for_first_power_ranking": num_tied_for_first_power_ranking,
            "weekly_points_by_position_data": weekly_points_by_position_data,
            "bad_boy_results_data": bad_boy_results_data,
            "num_tied_bad_boys": num_tied_bad_boys,
            "tied_bad_boy_bool": num_tied_bad_boys > 0,
            "num_tied_for_first_bad_boy": num_tied_for_first_bad_boy,
            "tie_for_first_bad_boy": tie_for_first_bad_boy
        }

        return report_info_dict

    def create_pdf_report(self):

        chosen_week_ordered_team_names = []
        chosen_week_ordered_managers = []
        report_info_dict = {}

        time_series_points_data = []
        time_series_efficiency_data = []
        time_series_luck_data = []
        time_series_power_rank_data = []

        season_average_points_by_position_dict = collections.defaultdict(list)

        week_counter = 1
        while week_counter <= int(self.chosen_week):
            report_info_dict = self.calculate_metrics(chosen_week=str(week_counter))

            # create team data for charts
            teams_data_list = []
            team_results = report_info_dict.get("team_results")
            for team in team_results:
                temp_team_info = team_results.get(team)
                teams_data_list.append([
                    temp_team_info.get("team_id"),
                    team,
                    temp_team_info.get("manager"),
                    temp_team_info.get("score"),
                    temp_team_info.get("coaching_efficiency"),
                    temp_team_info.get("luck"),
                    temp_team_info.get("power_rank")
                ])

                weekly_team_info = report_info_dict.get("weekly_points_by_position_data")
                for weekly_info in weekly_team_info:
                    if weekly_info[0] == team:
                        season_average_points_by_position_dict[team].append(weekly_info[1])

            teams_data_list.sort(key=lambda x: int(x[0]))

            ordered_team_names = []
            ordered_team_managers = []
            weekly_points_data = []
            weekly_coaching_efficiency_data = []
            weekly_luck_data = []
            weekly_power_rank_data = []

            for team in teams_data_list:
                ordered_team_names.append(team[1])
                ordered_team_managers.append(team[2])
                weekly_points_data.append([int(week_counter), float(team[3])])
                weekly_coaching_efficiency_data.append([int(week_counter), team[4]])
                weekly_luck_data.append([int(week_counter), float(team[5])])
                weekly_power_rank_data.append([int(week_counter), team[6]])

            chosen_week_ordered_team_names = ordered_team_names
            chosen_week_ordered_managers = ordered_team_managers

            if week_counter == 1:
                for team_points in weekly_points_data:
                    time_series_points_data.append([team_points])
                for team_efficiency in weekly_coaching_efficiency_data:
                    time_series_efficiency_data.append([team_efficiency])
                for team_luck in weekly_luck_data:
                    time_series_luck_data.append([team_luck])
                for team_power_rank in weekly_power_rank_data:
                    time_series_power_rank_data.append([team_power_rank])
            else:
                for index, team_points in enumerate(weekly_points_data):
                    time_series_points_data[index].append(team_points)
                for index, team_efficiency in enumerate(weekly_coaching_efficiency_data):
                    if float(team_efficiency[1]) != 0.0:
                        time_series_efficiency_data[index].append(team_efficiency)
                for index, team_luck in enumerate(weekly_luck_data):
                    time_series_luck_data[index].append(team_luck)
                for index, team_power_rank in enumerate(weekly_power_rank_data):
                    time_series_power_rank_data[index].append(team_power_rank)
            week_counter += 1

        # calculate season average metrics and then add columns for them to their respective metric table data
        season_average_calculator = SeasonAverageCalculator(chosen_week_ordered_team_names, report_info_dict)
        report_info_dict["score_results_data"] = season_average_calculator.get_average(
            time_series_points_data, "score_results_data", with_percent_bool=False)
        report_info_dict["coaching_efficiency_results_data"] = season_average_calculator.get_average(
            time_series_efficiency_data, "coaching_efficiency_results_data", with_percent_bool=True)
        report_info_dict["luck_results_data"] = season_average_calculator.get_average(time_series_luck_data,
                                                                                      "luck_results_data",
                                                                                      with_percent_bool=True)
        report_info_dict["power_ranking_results_data"] = season_average_calculator.get_average(
            time_series_power_rank_data, "power_ranking_results_data", with_percent_bool=False, bench_column_bool=False,
            reverse_bool=False)

        line_chart_data_list = [chosen_week_ordered_team_names,
                                chosen_week_ordered_managers,
                                time_series_points_data,
                                time_series_efficiency_data,
                                time_series_luck_data,
                                time_series_power_rank_data]

        # calculate season average points by position and add them to the report_info_dict
        PointsByPosition.calculate_points_by_position_season_averages(season_average_points_by_position_dict,
                                                                      report_info_dict)

        league_name = self.datasource.get_league_name()
        filename = league_name.replace(" ", "-") + "(" + self.league_id + ")_week-" + self.chosen_week + "_report.pdf"
        report_save_dir = self.config.get("Fantasy_Football_Report_Settings", "report_directory_base_path") + "/" + \
            league_name.replace(" ", "-") + "(" + self.league_id + ")"
        report_title_text = league_name + " (" + self.league_id + ") Week " + self.chosen_week + " Report"
        report_footer_text = \
            "<para alignment='center'>Report generated %s for Yahoo Fantasy Football league '%s' (%s).</para>" % \
            ("{:%Y-%b-%d %H:%M:%S}".format(datetime.datetime.now()), league_name, self.league_id)

        if not os.path.isdir(report_save_dir):
            os.makedirs(report_save_dir)

        if not self.test_bool:
            filename_with_path = os.path.join(report_save_dir, filename)
        else:
            filename_with_path = os.path.join(
                self.config.get("Fantasy_Football_Report_Settings", "report_directory_base_path") + "/",
                "test_report.pdf")

        # instantiate pdf generator
        pdf_generator = PdfGenerator(
            league_id=self.league_id,
            report_title_text=report_title_text,
            standings_title_text="League Standings",
            scores_title_text="Team Score Rankings",
            coaching_efficiency_title_text="Team Coaching Efficiency Rankings",
            luck_title_text="Team Luck Rankings",
            power_ranking_title_text="Team Power Rankings",
            report_footer_text=report_footer_text,
            report_info_dict=report_info_dict,
            bad_boy_title_text="Bad Boy Scoring"
        )

        # generate pdf of report
        file_for_upload = pdf_generator.generate_pdf(filename_with_path, line_chart_data_list)

        print("...SUCCESS! Generated PDF: {}\n".format(file_for_upload))

        return file_for_upload
