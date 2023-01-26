# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

ACCT = util.ACCOUNT_NUMBER

ADDR = util.NORMALIZED_ADDRESS
STREET_NUMBER = util.NORMALIZED_STREET_NUMBER
STREET_NAME = util.NORMALIZED_STREET_NAME
OCCUPANCY = util.NORMALIZED_OCCUPANCY
ADDITIONAL = util.NORMALIZED_ADDITIONAL_INFO

COLUMNS = [ADDR, STREET_NUMBER, STREET_NAME, OCCUPANCY]

TABLE_NAMES = \
[
    'BuildingPermits_L',
    'BuildingPermits_L_Cga',
    'BuildingPermits_L_Roof',
    'BuildingPermits_L_Siding',
    'BuildingPermits_L_Solar',
    'BuildingPermits_L_Sunrun',
    'BuildingPermits_L_Wx',
    'Businesses_L',
    'Census_L',
    'GlcacJobs_L',
]

TOTAL_COUNT = 'total_count'
TABLE_COUNT = 'table_count'

# Get unmatched addresses from specified building permits table
def get_unmatched( df_unmatched, table_name ):

    # Read specified table of building permits
    df = pd.read_sql_table( table_name, engine, index_col=util.ID, parse_dates=True )

    # Isolate rows without account number
    df = df[ df[ACCT].isnull() ]

    # Extract address information
    df = df[COLUMNS + ([ADDITIONAL] if ADDITIONAL in df.columns else [])]

    # Track source of unmatched addresses
    df[TABLE_COUNT] = table_name

    # Add to list of unmatched addresses
    df_unmatched = pd.concat( [df_unmatched, df] )

    return df_unmatched


##################################

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Generate table of unmatched addresses' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Initialize empty dataframe
    df_unmatched = pd.DataFrame( columns=COLUMNS )

    # Iterate through tables
    for table_name in TABLE_NAMES:
        df_unmatched = get_unmatched( df_unmatched, table_name )

    # Aggregate information about each address
    for idx, df_group in df_unmatched.groupby( by=[ADDR] ):

        # Count occurrences of this unmatched address
        df_unmatched.at[df_group.index[0], TOTAL_COUNT] = len( df_group )

        # Track which tables contain this unmatched address
        ls_table_counts = []
        for idx_table, df_group_table in df_group.groupby( by=[TABLE_COUNT] ):
            ls_table_counts.append( '{}: {}'.format( idx_table, len(df_group_table) ) )
        df_unmatched.at[df_group.index[0], TABLE_COUNT] = ', '.join( ls_table_counts )

    # Retain rows that contain aggregate information
    df_unmatched = df_unmatched.dropna( subset=[TOTAL_COUNT] )

    # Sort
    df_unmatched = df_unmatched.sort_values( by=[ADDR], ascending=False )

    # Convert floating point
    df_unmatched[TOTAL_COUNT] = df_unmatched[TOTAL_COUNT].astype( int )

    # Save final table of commercial assessments
    util.create_table( 'UnmatchedAddresses_L', conn, cur, df=df_unmatched )

    util.report_elapsed_time()
