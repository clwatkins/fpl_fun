"""Extracts features for match prediction algorithm.

"""

import pandas as pd
import typing


def get_player_vector():
    pass


def get_last_played_vector(season_matches_dfs: typing.List[pd.DataFrame]) -> pd.Series:
    """

    :param season_matches_dfs:
    :return:
    """
    df_concat = pd.concat(season_matches_dfs)

    sorted_df = df_concat.groupby(['TeamName', 'MatchDate']).max().sort_values(by='MatchDate', ascending=True)


