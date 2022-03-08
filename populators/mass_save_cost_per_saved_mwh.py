# Copyright 2022 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

#------------------------------------------------------

if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Calculate statistics on cost per saved mwh' )
    parser.add_argument( '-d', dest='db_filename',  help='Database filename' )
    parser.add_argument( '-t', dest='table',  help='Summary table' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read Analysis table from database
    df_analysis = pd.read_sql_table( 'Analysis', engine, index_col=util.ID )
    df_analysis = df_analysis[ [util.YEAR, util.TOWN_NAME, util.SECTOR, util.ANNUAL_ELECTRIC_USAGE, util.ANNUAL_ELECTRIC_SAVINGS, util.INCENTIVES_PER_SAVED_MWH] ]

    # Initialize empty dataframe
    cost_columns = \
    [
        util.YEAR,
        util.SECTOR,
        util.TOTAL_ANNUAL_ELECTRIC_USAGE,
        util.TOTAL_ANNUAL_ELECTRIC_SAVINGS,
        util.MEDIAN_INCENTIVES_PER_SAVED_MWH,
        util.AVG_INCENTIVES_PER_SAVED_MWH,
        util.STD_INCENTIVES_PER_SAVED_MWH,
        util.MWH_SAVED_AS_PCT_OF_USED,
    ]
    df_cost = pd.DataFrame( columns=cost_columns )

    # Iterate over analysis dataframe
    for idx, df_group in df_analysis.groupby( by=[util.YEAR, util.SECTOR] ):

        # Calculate statistics for current year and sector
        used = df_group[util.ANNUAL_ELECTRIC_USAGE].sum()
        saved = df_group[util.ANNUAL_ELECTRIC_SAVINGS].sum()
        nz_column = util.nonzero( df_group, util.INCENTIVES_PER_SAVED_MWH )
        med = nz_column.median()
        avg = nz_column.mean()
        std = nz_column.std()
        pct = ( 100 * saved / used ) if used else 0

        # Create row with new values
        dc_row = \
        {
            util.YEAR: df_group.iloc[0][util.YEAR],
            util.SECTOR: df_group.iloc[0][util.SECTOR],
            util.TOTAL_ANNUAL_ELECTRIC_USAGE: used,
            util.TOTAL_ANNUAL_ELECTRIC_SAVINGS: saved,
            util.MEDIAN_INCENTIVES_PER_SAVED_MWH: med,
            util.AVG_INCENTIVES_PER_SAVED_MWH: avg,
            util.STD_INCENTIVES_PER_SAVED_MWH: std,
            util.MWH_SAVED_AS_PCT_OF_USED: pct,
        }

        # Add row to result dataframe
        df_row = pd.DataFrame( [dc_row] )
        df_cost = pd.concat( [df_cost, df_row] )

    # Save results to database
    util.create_table( args.table, conn, cur, df=df_cost )

    # Report elapsed time
    util.report_elapsed_time()
