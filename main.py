import pandas as pd, numpy as np, copy
pd.options.display.float_format = '{:,}'.format
from datetime import datetime
from datetime import timedelta
import yfpy
pd.set_option('mode.chained_assignment', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
np.seterr(divide='ignore')
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings('error')
import logging
logging.getLogger("yfpy.query").setLevel(level=logging.INFO)
import json
from libraries import weeks_parser, online_data_stitcher, natStatTrick_parser, hockeyReference_parser, rosterStatsQuery
from libraries import yahoo_data_collection, online_data_parser, matchup_metadata, clean_player_name_parser, matchup_consolidator, google_sheets_trunc_and_load
from libraries import teams_metadata, scheduleParser, player_parser, close_matchup_wrapper, parsed_data_stitcher
from libraries import fp_calculator, matchup_data_cruncher, trans_and_draft, player_metadata_parser, fake_sql_database_creator
from libraries import draft_analytics,faab_analytics,keeper_analytics,bench_analytics,streamer_analytics,forgotten_start_analytics,chrono_trigger
import time as time
import os
root_dir = 'C:/Users/16472/PycharmProjects/Hockey_FantaPy/'
time_start = time.time()
print("GOOD DAY! FANTASY HOCKEY 2023 VERSION")
with open(f'{root_dir}control_daily.json', 'r') as f:
#with open(f'{root_dir}control.json', 'r') as f:
    control_file = json.loads(f.read())
today = (datetime.now()).strftime('%Y-%m-%d')
yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
print(f'Today: {today} >><< Yesterday: {yesterday}')
yearsToCheck = control_file['Years']  # .keys()
run_type = control_file['run_type']
yearsToCheck = [int(x) for x in yearsToCheck.keys() if yearsToCheck[x]['status'] == "RUN"]
print(f'Years to check: {yearsToCheck}')
# ============================================================================================
for year in yearsToCheck:
    print(f'>> Starting up data parsing for {year}')
    weeks_parser(year, control_file)
    scheduleParser(year, control_file)

    all_dates = chrono_trigger(year, run_type, control_file)

    # yahoo season week date info

    # yahoo team metadata
    teams_metadata(year, control_file)

    # NHL website schedule parser (and yahoo week overlay)

    # Weekly Matchup metadata parser
    matchup_metadata(year, control_file)

    # Yahoo Player Metadata parser
    player_metadata_parser(year, control_file)

    # transactions and draft info
    trans_and_draft(year, control_file)

    # need to find a way to clean this
    # clean_player_name_parser(control_file['Player Name Cleaner'],yearsToCheck)

    # Get the NST, HR, and Yahoo rosters (raw)
    online_data_parser(year, all_dates, control_file)

    # stitch the data here if need be
    parsed_data_stitcher(year, all_dates, control_file)

    # calculate all the FP stuff on the stitched datafiles
    fp_calculator(year, all_dates, control_file)

    fake_sql_database_creator(year, all_dates, control_file)

    draft_analytics(year, control_file)
    faab_analytics(year, control_file)
    keeper_analytics(year, control_file)
    streamer_analytics(year, control_file)
    bench_analytics(year, control_file)
    forgotten_start_analytics(year, control_file)

    matchup_consolidator(year, control_file)

# ============================================================================================
# Consolidation-level stuff (ie, stitch all the stuff together no matter what years are selected)

google_sheets_trunc_and_load(control_file)

time_end = time.time()

print(f">>>> [Rundate: {time.ctime()}] Finished in {time_end - time_start}s! See ya!")



