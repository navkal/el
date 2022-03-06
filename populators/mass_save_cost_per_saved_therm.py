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
    parser = argparse.ArgumentParser( description='Calculate statistics on cost per saved therm' )
    parser.add_argument( '-d', dest='db_filename',  help='Database filename' )
    parser.add_argument( '-t', dest='table',  help='Summary table' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read Analysis table from database
    df_analysis = pd.read_sql_table( 'Analysis', engine, index_col=util.ID )
    df_analysis = df_analysis[ [util.YEAR, util.TOWN_NAME, util.SECTOR, util.INCENTIVES_PER_SAVED_THERM] ]

    # Initialize empty dataframe
    df_cost = pd.DataFrame( columns=[util.YEAR, util.SECTOR, util.MEDIAN_INCENTIVES_PER_SAVED_THERM, util.AVG_INCENTIVES_PER_SAVED_THERM, util.STD_INCENTIVES_PER_SAVED_THERM] )

    # Iterate over analysis dataframe
    for idx, df_group in df_analysis.groupby( by=[util.YEAR, util.SECTOR] ):

        # Calculate statistics for current year and sector
        med = df_group[util.INCENTIVES_PER_SAVED_THERM].median()
        avg = df_group[util.INCENTIVES_PER_SAVED_THERM].mean()
        std = df_group[util.INCENTIVES_PER_SAVED_THERM].std()

        # Create row with new values
        dc_row = \
        {
            util.YEAR: df_group.iloc[0][util.YEAR],
            util.SECTOR: df_group.iloc[0][util.SECTOR],
            util.MEDIAN_INCENTIVES_PER_SAVED_THERM: med,
            util.AVG_INCENTIVES_PER_SAVED_THERM: avg,
            util.STD_INCENTIVES_PER_SAVED_THERM: std,
        }

        # Add row to final dataframe
        df_row = pd.DataFrame( [dc_row] )
        df_cost = pd.concat( [df_cost, df_row] )

    # Save results to database
    util.create_table( args.table, conn, cur, df=df_cost )

    # Report elapsed time
    util.report_elapsed_time()
