"""Extracts features for match prediction algorithm.

"""

from Analytics.utils import flatten, compute_diff

import pandas as pd
import numpy as np
import typing


def create_feature_vector():
    """Function to create combined dataframe to train on match outcomes.

    Features are all normalised, encoded, etc.

    Includes a number of features:
    1. Match outcome (W / D / L)
    2. Team points, goals for, goals against at that point in time
    3. Opponent points, goals for, goals against
    4. Result last time played that team
    5. Team starting 11 player rating
    6. Opponent starting 11 player rating
    7. Team formation (encoded)
    8. Opponent formation (encoded)
    9. Team days since last match
    10. Opponent days since last match
    11. Match time of day
    12. Match outcome at t-1
    13. Match outcome at t-2
    14. Match outcome at t-3
    """

    pass


def get_match_lineup_vector(matches_df: pd.DataFrame, season_detail_df: pd.DataFrame) -> pd.Series:
    def _find_lineup(match_id: str, home_or_away: str) -> list:
        lineup_s = season_detail_df[season_detail_df.Id == match_id][[f'{home_or_away}LineupGoalkeeper',
                                                                     f'{home_or_away}LineupDefense',
                                                                     f'{home_or_away}LineupForward',
                                                                     f'{home_or_away}LineupMidfield']]

        lineup_deep_l = [_.split(';') for _ in lineup_s.values[0]]
        lineup_l = flatten(lineup_deep_l)
        return [_.strip() for _ in lineup_l if _ != '']

    return matches_df.apply(lambda x: _find_lineup(x.MatchId, x.HomeOrAway))


def get_encoded_formation_vector(matches_df: pd.DataFrame, season_detail_df: pd.DataFrame) -> pd.Series:
    def _find_formation(match_id: str, home_or_away: str) -> str:
        return season_detail_df[season_detail_df.Id == match_id][f'{home_or_away}TeamFormation']

    return matches_df.apply(lambda x: _find_formation(x.MatchId, x.HomeOrAway))


def get_last_played_match_vector(season_matches_dfs: typing.List[pd.DataFrame]) -> pd.Series:
    """

    :param season_matches_dfs:
    :return:
    """
    df_concat = pd.concat(season_matches_dfs)

    sorted_df = df_concat.sort_values(by=['TeamName', 'MatchDate'], ascending=True)

    date_diff_df = sorted_df.groupby('TeamName').MatchDate.diff()

    # check we're seeing as many null values as expected (should be = n of unique teams in dataset)
    unique_teams = df_concat.TeamNames.nunique()
    null_vals = date_diff_df[np.isnat(date_diff_df.values)].shape[0]

    assert unique_teams == null_vals

    date_diff_df = date_diff_df.fillna(0)
    return date_diff_df.dt.total_seconds()


def get_last_played_team_vector():
    pass


def get_lineup_stats_vector(team_match_lineup_s: pd.Series, player_stats_df: pd.DataFrame) -> pd.Series:
    """Finds the overall rating of a player from FIFA player statistics.

    1. Builds a lookup column on player and team name
    2. Computes a difflib ratio between the lookup name and every entry in our lookup col
    3. Returns the overall rating of the player with the highest difflib ratio

    WARNING: depending on the FIFA datasource used, column names may need to be modified.

    Example player dataset: https://github.com/HashirZahir/FIFA-Player-Ratings

    :param team_match_lineup_s: Series composed of a list of player names representing a starting lineup
    :param player_stats_df: FIFA player-based stats dataframe
    :return: Series composed of a list of player overall ratings representing a starting lineup
    """

    key_matching_col = player_stats_df.Name.str.cat(player_stats_df.Club, sep=' ')

    def _lookup_player_name_loc(player_name):
        return key_matching_col.apply(compute_diff, args=(player_name,)).idxmax()

    def _get_player_rating(player_name):
        return player_stats_df.loc[_lookup_player_name_loc(player_name)].Overall

    def get_player_ratings(player_name_list):
        return [_get_player_rating(player) for player in player_name_list]

    return team_match_lineup_s.apply(get_player_ratings)
