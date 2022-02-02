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
    parser = argparse.ArgumentParser( description='Summarize MassSave analysis' )
    parser.add_argument( '-d', dest='db_filename',  help='Database filename' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read table from database
    df_analysis = pd.read_sql_table( 'Analysis', engine, index_col=util.ID )
    df_analysis = df_analysis[ [util.YEAR, util.TOWN_NAME, util.SECTOR, util.COMBINED_EES_MINUS_INCENTIVES] ]

    # Determine column names
    column_name_map = { util.TOWN_NAME: util.TOWN_NAME }
    years = df_analysis[util.YEAR].sort_values().drop_duplicates().tolist()

    for year in years:
        column_name_map[year] = year + '_to_' + years[-1]

    # Create empty summary dataframe
    df_summary = pd.DataFrame( columns=column_name_map.keys() )

    # Iterate over analysis rows, grouped by town
    for idx, df_group in df_analysis.groupby( by=[util.TOWN_NAME] ):

        # Initialize summary row for current town
        summary_row = pd.Series( dtype=object )
        summary_row[util.TOWN_NAME] = df_group.iloc[0][util.TOWN_NAME]

        # Select 'Total' rows from group
        df_group = df_group[df_group[util.SECTOR] == util.SECTOR_TOTAL]

        # Iterate over totals
        for index, row in df_group.iterrows():

            # Select time range
            df_range = df_group[ df_group[util.YEAR] >= row[util.YEAR] ]
            df_range = df_range.reset_index( drop=True )

            # Calculate average annual loss over range
            sum = int( df_range[util.COMBINED_EES_MINUS_INCENTIVES].sum() )
            avg = int( sum / len( df_range ) )

            # Save average in summary row
            summary_row[df_range.at[0, util.YEAR]] = avg

        # Save summary row in dataframe
        df_summary = df_summary.append( summary_row, ignore_index=True )

    # Fix column names and datatypes
    df_summary = df_summary.rename( columns=column_name_map )
    df_summary = df_summary.fillna( 0 )
    for column in df_summary.columns[1:]:
        df_summary[column] = df_summary[column].astype(int)

    # Save summary results to database
    util.create_table( "AverageAnnualEesMinusIncentives", conn, cur, df=df_summary )

    # Report elapsed time
    util.report_elapsed_time()
