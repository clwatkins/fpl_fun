"""Reconstructs a football season fixture by fixture, outputting the table as it stood for every event.

Uses the football data API to provide raw match data.

Documentation here: http://www.xmlsoccer.com/FootballData.asmx

Key values:
EPL:
UEFA Champions League:
FA Cup:
FA Community Shield:

Get competitions: http://api.football-data.org/v2/competitions/
Get competition seasons: http://api.football-data.org/v2/competitions/2021
Get season info:
"""

import requests as rq
import pandas as pd
import os
from lxml import etree
import typing


class XmlSoccerRequest(object):
    def __init__(self):
        self.api_key = os.environ['XML_SOCCER_API_KEY']
        self.api_url = 'http://www.xmlsoccer.com/FootballDataDemo.asmx/'
        self.base_params = {'ApiKey': self.api_key}

    def get(self, method:str='GetAllLeagues', **kwargs) -> typing.List[typing.Dict]:
        r = rq.get(self.api_url + method,
                   params={**self.base_params, **kwargs})

        if r.status_code != 200:
            raise ConnectionError(r.text)
        else:
            return self._process_xml(r)

    @staticmethod
    def _process_xml(response: rq.Response) -> typing.List[typing.Dict]:
        data = []

        try:
            root = etree.XML(response.text.encode('utf-8'))
        except SyntaxError:
            raise SyntaxError(response.text)

        if len(root) == 0:
            raise (ConnectionError(root.text))
        for child in list(root):
            tmp = dict()
            for element in list(child):
                tmp[element.tag] = element.text
            data.append(tmp)

        return data


def get_season_matches(league_code: str, season_date_string: str) -> pd.DataFrame:
    """Downloads matches from a particular competition and season into a dataframe.

    # Common competition codes


    E.g. for the Premier League 17-18 season I would use:
    competition_code=2021
    season_name='1718'

    :param league_code: int
    :param season_date_string: str
    :return: df containing all of the competition season match information
    """
    season_matches = XmlSoccerRequest().get('GetFixturesByLeagueAndSeason',
                                            league=league_code,
                                            seasonDateString=season_date_string)

    season_matches_df = pd.DataFrame.from_records(season_matches)
    season_matches_df['MatchDate'] = pd.to_datetime(season_matches_df.Date)
    season_matches_df['CompetitionSeason'] = season_date_string
    season_matches_df['CompetitionName'] = season_matches[0]['League']

    return season_matches_df


def process_season_matches(season_matches_df: pd.DataFrame) -> typing.Tuple[pd.DataFrame, typing.Any]:
    """Processes raw season match data into parsable match and table data.

    :param season_matches_df: Dataframe as returned by get_season_matches function
    :return: 3 dataframes: expanded_df with match info, table_df with match outcome info, and grouped_table_df
    with a view of the league table week on week through the season
    """

    def create_table_records(row):
        # TODO handle null values better (don't contaminate df)
        home_row_data = dict()
        home_row_data['TeamName'] = row.HomeTeam
        home_row_data['HomeOrAway'] = 'Home'
        home_row_data['GoalsFor'] = float(row.HomeGoals)
        home_row_data['GoalsAgainst'] = float(row.AwayGoals)
        home_row_data['MatchDay'] = float(row.Round)
        home_row_data['MatchId'] = row.Id
        home_row_data['GoalDiff'] = float(row.HomeGoals) - float(row.AwayGoals)
        home_row_data['GamesPlayed'] = 1
        home_row_data['CompetitionSeason'] = row.CompetitionSeason
        home_row_data['CompetitionName'] = row.CompetitionName
        home_row_data['MatchDate'] = row.MatchDate

        if home_row_data['GoalDiff'] > 0:
            points = 3
            home_row_data['GamesWon'] = 1
        elif home_row_data['GoalDiff'] == 0:
            points = 1
            home_row_data['GamesDrawn'] = 1
        else:
            points = 0
            home_row_data['GamesLost'] = 1

        home_row_data['Points'] = points

        # repeat for away team
        away_row_data = dict()
        away_row_data['TeamName'] = row.AwayTeam
        away_row_data['HomeOrAway'] = 'Away'
        away_row_data['GoalsFor'] = float(row.AwayGoals)
        away_row_data['GoalsAgainst'] = float(row.HomeGoals)
        away_row_data['MatchDay'] = float(row.Round)
        away_row_data['MatchId'] = row.Id
        away_row_data['GoalDiff'] = float(row.AwayGoals) - float(row.HomeGoals)
        away_row_data['GamesPlayed'] = 1
        away_row_data['CompetitionSeason'] = row.CompetitionSeason
        away_row_data['CompetitionName'] = row.CompetitionName
        away_row_data['MatchDate'] = row.MatchDate

        if away_row_data['GoalDiff'] > 0:
            points = 3
            away_row_data['GamesWon'] = 1
        elif away_row_data['GoalDiff'] == 0:
            points = 1
            away_row_data['GamesDrawn'] = 1
        else:
            points = 0
            away_row_data['GamesLost'] = 1

        away_row_data['Points'] = points

        return [home_row_data, away_row_data]

    season_matches_dropped_df = season_matches_df.dropna(thresh=10)  # drop only records that are substantively blank
    table_df_deep_list = season_matches_dropped_df.apply(create_table_records, axis=1)
    table_df_flat_list = [l for sublist in table_df_deep_list for l in sublist]
    table_df = pd.DataFrame.from_records(table_df_flat_list)

    grouped_table_df = table_df.groupby(['MatchDay', 'TeamName']).sum().groupby('TeamName').cumsum()\
        .sort_values(by=['MatchDay', 'Points', 'GoalDiff'])

    return table_df, grouped_table_df
