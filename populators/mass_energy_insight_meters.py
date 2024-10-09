# Copyright 2024 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

DAYS_PER_MONTH = 365/12

COLUMNS = \
[
    util.ACCOUNT_NUMBER,
    util.READ_DAYS,
]

CALC_COLUMNS = \
[
    util.TOTAL_KWH,
    util.BILLED_PEAK_KW,
    util.BILLED_ON_PEAK_KW,
    util.TOU_ON_PEAK_KWH,
    util.TOU_OFF_PEAK_KWH,
    util.RKVA,
    util.UTILITY_CHARGES,
    util.SUPPLIER_CHARGES,
    util.TOTAL_CHARGES,
    util.LATE_PAYMENT,
]

_M = '_m'
_Y1 = '_y1'
_Y2 = '_y2'
_Y3 = '_y3'

for col in CALC_COLUMNS:
    COLUMNS.append( col + _M )
    COLUMNS.append( col + _Y1 )
    COLUMNS.append( col + _Y2 )
    COLUMNS.append( col + _Y3 )

# Columns describing facility that owns each electric meter
FACILITY_COLUMNS = \
[
    util.ACCOUNT_NUMBER,
    util.DEPARTMENT,
    util.FACILITY,
]

#------------------------------------------------------

if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Summarize bulk electric meter data obtained from National Grid' )
    parser.add_argument( '-d', dest='db_filename', help='Database filename' )
    parser.add_argument( '-i', dest='input_table', help='Input table name' )
    parser.add_argument( '-f', dest='facility_table', help='Input table from which to merge facility information' )
    parser.add_argument( '-o', dest='output_table', help='Output table name' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read table from database
    df_raw = pd.read_sql_table( args.input_table, engine, index_col=util.ID )

    # Initialize empty summary dataframe
    df_summary = pd.DataFrame( columns=COLUMNS )

    # Build summary
    for idx, df_group in df_raw.groupby( by=[util.ACCOUNT_NUMBER] ):

        # Initialize empty summary row
        summary_row = dict( ( el, 0 ) for el in COLUMNS )

        # Populate row summarizing an individual electric meter
        summary_row[util.ACCOUNT_NUMBER] = idx
        read_days = df_group[util.READ_DAYS].sum()
        summary_row[util.READ_DAYS] = read_days

        for col in CALC_COLUMNS:

            # Monthly average
            summary_row[col + _M] = int( df_group[col].sum() * DAYS_PER_MONTH / read_days )

            # Past year
            suffix = _Y1
            df_year = df_group.iloc[-12:]
            if len( df_year ) == 12:
                summary_row[col + suffix] = int( df_year[col].sum() )

            # Preceding year
            suffix = _Y2
            df_year = df_group.iloc[-24:-12]
            if len( df_year ) == 12:
                summary_row[col + suffix] = int( df_year[col].sum() )

            # Past preceding year
            suffix = _Y3
            df_year = df_group.iloc[-36:-24]
            if len( df_year ) == 12:
                summary_row[col + suffix] = int( df_year[col].sum() )

        # Append row to dataframe
        df_summary = df_summary.append( summary_row, ignore_index=True )

    # Read table containing facility information from database
    df_facility = pd.read_sql_table( args.facility_table, engine, index_col=util.ID )

    # Prepare facility table for merge
    df_facility = df_facility[ df_facility[util.PROVIDER] == util.NATIONAL_GRID ]
    df_facility = df_facility.drop_duplicates( subset=[util.ACCOUNT_NUMBER] )
    df_facility = df_facility[FACILITY_COLUMNS]
    df_facility = util.fix_numeric_columns( df_facility )

    # Merge facility information into the summary table
    df_summary = pd.merge( df_summary, df_facility, how='left', on=[util.ACCOUNT_NUMBER] )

    # Make sure numeric values are not saved as text
    df_summary = util.fix_numeric_columns( df_summary )

    # Save result to database
    util.create_table( args.output_table, conn, cur, df=df_summary )

    # Report elapsed time
    util.report_elapsed_time()
