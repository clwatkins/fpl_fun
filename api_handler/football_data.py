"""Reconstructs a football season fixture by fixture, outputting the table as it stood for every event.

Uses the football data API to provide raw match data.

Documentation here: https://www.football-data.org/documentation/api

Key values:
EPL: 2021
UEFA Champions League: 2001
FA Cup: 2055
FA Community Shield: 2056

18-19 season: 151
17-18 season: 23
16-17 season: 256

Get competitions: http://api.football-data.org/v2/competitions/
Get competition seasons: http://api.football-data.org/v2/competitions/2021
Get season info:
"""

import requests as rq
import pandas as pd
import os

FOOTBALL_DATA_API_KEY = os.environ['FOOTBALL_DATA_API_KEY']


def get_season_matches(competition_code, year_start, year_end, season_name):
    """Downloads matches from a particular competition and season into a dataframe.

    # Common competition codes
    EPL: 2021
    UEFA Champions League: 2001
    FA Cup: 2055
    FA Community Shield: 2056

    E.g. for the Premier League 17-18 season I would use:
    competition_code=2021
    year_start=2017
    year_end=2018
    season_name='17-18'

    :param competition_code: int
    :param year_start: int
    :param year_end: int
    :param season_name: str
    :return: df containing all of the competition season match information
    """
    season_matches_r = rq.get(f'http://api.football-data.org/v2/competitions/{competition_code}/matches/',
                             params={
                                 'dateFrom': f'{year_start}-08-01',
                                 'dateTo': f'{year_end}-05-30'
                             },
                             headers={'X-Auth-Token': FOOTBALL_DATA_API_KEY})

    if season_matches_r.status_code != 200:
        print('\n')
        print(season_matches_r.json())
        print(season_matches_r.url)

        raise ConnectionError(season_matches_r.status_code)

    season_matches_df = pd.DataFrame.from_records(season_matches_r.json()['matches'])
    season_matches_df['season'] = season_name
    season_matches_df['competitionName'] = season_matches_r.json()['competition']['name']

    return season_matches_df


def process_season_matches(season_matches_df):
    """Processes raw season match data into parsable match and table data.

    :param season_matches_df: Dataframe as returned by get_season_matches function
    :return: 3 dataframes: expanded_df with match info, table_df with match outcome info, and grouped_table_df
    with a view of the league table week on week through the season
    """

    def expand_raw_fields(row):
        row_data = dict()
        row_data['awayTeamName'] = row.awayTeam['name']
        row_data['homeTeamName'] = row.homeTeam['name']
        row_data['matchId'] = row.id
        row_data['matchDateTime'] = row.utcDate
        row_data['homeScore'] = row.score['fullTime']['homeTeam']
        row_data['awayScore'] = row.score['fullTime']['awayTeam']
        row_data['matchDay'] = row.matchday
        row_data['season'] = row.season
        row_data['competition'] = row.competitionName

        return row_data

    def create_table_records(row):
        home_row_data = dict()
        home_row_data['teamName'] = row.homeTeamName
        home_row_data['homeOrAway'] = 'home'
        home_row_data['goalsFor'] = row.homeScore
        home_row_data['goalsAgainst'] = row.awayScore
        home_row_data['matchDay'] = row.matchDay
        home_row_data['matchId'] = row.matchId
        home_row_data['goalDiff'] = row.homeScore - row.awayScore
        home_row_data['played'] = 1
        home_row_data['season'] = row.season
        home_row_data['competition'] = row.competitionName

        if home_row_data['goalDiff'] > 0:
            points = 3
            home_row_data['gamesWon'] = 1
        elif home_row_data['goalDiff'] == 0:
            points = 1
            home_row_data['gamesDrawn'] = 1
        else:
            points = 0
            home_row_data['gamesLost'] = 1

        home_row_data['points'] = points

        # repeat for away team
        away_row_data = dict()
        away_row_data['teamName'] = row.awayTeamName
        away_row_data['homeOrAway'] = 'away'
        away_row_data['goalsFor'] = row.awayScore
        away_row_data['goalsAgainst'] = row.homeScore
        away_row_data['matchDay'] = row.matchDay
        away_row_data['matchId'] = row.matchId
        away_row_data['goalDiff'] = row.awayScore - row.homeScore
        away_row_data['played'] = 1
        away_row_data['season'] = row.season
        away_row_data['competition'] = row.competitionName

        if away_row_data['goalDiff'] > 0:
            points = 3
            away_row_data['gamesWon'] = 1
        elif away_row_data['goalDiff'] == 0:
            points = 1
            away_row_data['gamesDrawn'] = 1
        else:
            points = 0
            away_row_data['gamesLost'] = 1

        away_row_data['points'] = points

        return [home_row_data, away_row_data]

    expanded_df_dict = season_matches_df.apply(expand_raw_fields, axis=1)
    expanded_df = pd.DataFrame.from_records(expanded_df_dict)
    expanded_df['matchDateTime'] = pd.to_datetime(expanded_df.matchDateTime)

    table_df_deep_list = expanded_df.apply(create_table_records, axis=1)
    table_df_flat_list = [l for sublist in table_df_deep_list for l in sublist]
    table_df = pd.DataFrame.from_records(table_df_flat_list)

    grouped_table_df = table_df.groupby(['matchDay', 'teamName']).max().groupby('teamName').cumsum()

    return expanded_df, table_df, grouped_table_df
