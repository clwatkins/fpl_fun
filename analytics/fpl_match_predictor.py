"""Reconstructs a football season fixture by fixture, outputting the table as it stood for every event.

Uses the football data API to provide raw match data.

Documentation here: https://www.football-data.org/documentation/api

Key values:
EPL competition code: 2021
18-19 season: 151
17-18 season: 23
16-17 season: 256

Get competitions: http://api.football-data.org/v2/competitions/
Get competition seasons: http://api.football-data.org/v2/competitions/2021
Get season info:
"""

import requests as rq
import json
import pandas as pd
import datetime as dt

