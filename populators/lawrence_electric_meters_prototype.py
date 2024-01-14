import pandas as pd

import re
import numpy as np

import warnings
import requests
from bs4 import BeautifulSoup

import sys
sys.path.append('../util')
import util
import vision
import printctl

from datetime import date
from dateutil.relativedelta import relativedelta

import os

db_filename = '../db/lawrence_electric_meters_prototype.sqlite'
db_tablename = 'RawElectricMeters_L'
os.system( 'python xl_to_db.py -d ../xl/mass_energy_insight/electric_meters_l -v -l account_number -s account_number,readDate -o {} -t {} -c'.format( db_filename, db_tablename ) )


# Open the database
conn, cur, engine = util.open_database( db_filename, False )

# Read table from database
df_raw = pd.read_sql_table( db_tablename, engine, parse_dates=True, index_col=util.ID )

# print( df_raw )
print( list( df_raw.columns ) )

DAYS_PER_MONTH = 365/12

COLUMNS = \
[
    util.ACCOUNT_NUMBER,
    util.START_DATE,
    util.END_DATE,
    util.ELAPSED_MONTHS,
]

# Initialize empty summary structures
df_summary = pd.DataFrame( columns=COLUMNS )
summary_row = dict( ( el, 0 ) for el in COLUMNS )

# Build summary
for idx, df_group in df_raw.groupby( by=[util.ACCOUNT_NUMBER] ):
    period_start = pd.Timestamp( df_group.iloc[0][util.READ_FROM_DATE] )
    period_end = pd.Timestamp( df_group.iloc[len(df_group)-1][util.READ_DATE] )
    delta = relativedelta( period_end, period_start )
    elapsed_months = ( delta.years * 12 ) + delta.months + round( delta.days / DAYS_PER_MONTH )

    summary_row[util.ACCOUNT_NUMBER] = idx
    summary_row[util.START_DATE] = period_start
    summary_row[util.END_DATE] = period_end
    summary_row[util.ELAPSED_MONTHS] = elapsed_months

    df_summary = df_summary.append( summary_row, ignore_index=True )

print( df_summary )

# Save result to database
util.create_table( 'ElectricMeters_L', conn, cur, df=df_summary )

exit()

