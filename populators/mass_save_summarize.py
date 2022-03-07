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
    parser.add_argument( '-s', dest='sector',  help='Sector to summarize' )
    parser.add_argument( '-t', dest='table',  help='Summary table' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read Towns table from database
    df_towns = pd.read_sql_table( 'Towns', engine, index_col=util.ID )

    # Read Analysis table from database
    df_analysis = pd.read_sql_table( 'Analysis', engine, index_col=util.ID )
    df_analysis = df_analysis[ [util.YEAR, util.TOWN_NAME, util.SECTOR, util.ANNUAL_ELECTRIC_USAGE, util.ANNUAL_ELECTRIC_SAVINGS, util.ANNUAL_GAS_USAGE, util.ANNUAL_GAS_SAVINGS, util.COMBINED_EES_IN, util.COMBINED_INCENTIVES_OUT, util.COMBINED_EES_MINUS_INCENTIVES] ]

    # Determine column names
    column_name_map = { util.TOWN_NAME: util.TOWN_NAME, util.POPULATION: util.POPULATION }
    years = df_analysis[util.YEAR].sort_values().drop_duplicates().tolist()
    last_year = int( years[-1] )

    for year in years:
        prefix = str( last_year - int( year ) + 1 ) + '_yr_'
        column_name_map[year + '$'] = prefix + 'ees_minus_incentives_$'
        column_name_map[year + '%'] = prefix + 'incentives_as_%_of_ees'
        column_name_map[year + 'mu'] = prefix + 'mwh_used_avg'
        column_name_map[year + 'ms'] = prefix + 'mwh_saved_avg'
        column_name_map[year + 'm%'] = prefix + util.MWH_SAVED_AS_PCT_OF_USED
        column_name_map[year + 'tu'] = prefix + 'therms_used_avg'
        column_name_map[year + 'ts'] = prefix + 'therms_saved_avg'
        column_name_map[year + 't%'] = prefix + util.THERMS_SAVED_AS_PCT_OF_USED

    # Create empty summary dataframe
    df_summary = pd.DataFrame( columns=column_name_map.keys() )

    # Iterate over analysis rows, grouped by town
    for idx, df_group in df_analysis.groupby( by=[util.TOWN_NAME] ):

        # Get current town
        town = df_group.iloc[0][util.TOWN_NAME]

        # Initialize summary row for current town
        summary_row = pd.Series( dtype=object )
        summary_row[util.TOWN_NAME] = town
        summary_row[util.POPULATION] = df_towns[df_towns[util.TOWN_NAME]==town][util.POPULATION].values[0]

        # Select 'Total' rows from group
        df_group = df_group[df_group[util.SECTOR] == args.sector]

        # Iterate over totals
        for index, row in df_group.iterrows():

            # Select time range
            df_range = df_group[ df_group[util.YEAR] >= row[util.YEAR] ]
            df_range = df_range.reset_index( drop=True )

            # Calculate summary statistics for this time range
            dol = int( df_range[util.COMBINED_EES_MINUS_INCENTIVES].sum() )
            ees = df_range[util.COMBINED_EES_IN].sum()
            pct = int( 100 * df_range[util.COMBINED_INCENTIVES_OUT].sum() / ees ) if ees else 0
            mwhu = int( df_range[util.ANNUAL_ELECTRIC_USAGE].mean() )
            mwhs = int( df_range[util.ANNUAL_ELECTRIC_SAVINGS].mean() )
            mwhp = round( 100 * mwhs / mwhu, 4 ) if mwhu else 0
            thmu = int( df_range[util.ANNUAL_GAS_USAGE].mean() )
            thms = int( df_range[util.ANNUAL_GAS_SAVINGS].mean() )
            thmp = round( 100 * thms / thmu, 4 ) if thmu else 0

            # Save statistics in summary row
            range_start = df_range.at[0, util.YEAR]
            summary_row[range_start + '$'] = dol
            summary_row[range_start + '%'] = pct
            summary_row[range_start + 'mu'] = mwhu
            summary_row[range_start + 'ms'] = mwhs
            summary_row[range_start + 'm%'] = mwhp
            summary_row[range_start + 'tu'] = thmu
            summary_row[range_start + 'ts'] = thms
            summary_row[range_start + 't%'] = thmp

        # Save summary row in dataframe
        df_summary = df_summary.append( summary_row, ignore_index=True )

    # Fix column names and datatypes
    df_summary = df_summary.rename( columns=column_name_map )
    df_summary = df_summary.fillna( 0 )

    # Save summary results to database
    util.create_table( args.table, conn, cur, df=df_summary )

    # Report elapsed time
    util.report_elapsed_time()
