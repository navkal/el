# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

ADDR = util.NORMALIZED_ADDRESS
STREET_NUMBER = util.NORMALIZED_STREET_NUMBER
STREET_NAME = util.NORMALIZED_STREET_NAME
OCCUPANCY = util.NORMALIZED_OCCUPANCY
ADDITIONAL = util.NORMALIZED_ADDITIONAL_INFO

STREET_ADDR = 'merge_addr'
SAVE_ADDR = 'save_' + ADDR



# Isolate entries that did not find matches when merged with permit data
def isolate_unmatched( df_merge, left_columns, df_result, column_name ):

    # Create dataframe of unmatched entries
    df_unmatched = df_merge.copy()
    df_unmatched = df_unmatched[ df_unmatched[column_name].isna() ]
    df_unmatched = df_unmatched[left_columns]

    # Clear unmatched entries out of merged data
    df_matched = df_merge.copy()
    df_matched = df_matched.dropna( subset=[column_name] )

    # Append matched entries to the result
    df_result = df_result.append( df_matched, ignore_index=True )

    # Report progress
    print( '---' )
    print( 'Matched: {}, Unmatched: {}'.format( df_matched.shape, df_unmatched.shape ) )
    print( 'Result: {}'.format( df_result.shape ) )

    return df_result, df_unmatched


def combine_tables( df_left, df_right ):

    # Prepare input dataframes for merge
    df_left[ADDR] = df_left[STREET_NUMBER] + ' ' + df_left[STREET_NAME]
    df_right[ADDR] = df_right[STREET_NUMBER] + ' ' + df_right[STREET_NAME]
    left_columns = [util.JOB_NUMBER, ADDR]
    right_columns = [util.PERMIT_NUMBER, ADDR]
    df_left = df_left[left_columns].copy()
    df_right =  df_right[right_columns].copy()

    # Initialize empty result
    df_result = pd.DataFrame()

    # Prepare for first step
    df_unmatched = df_left.copy()

    # Merge on unexpanded addresses
    df_merge = pd.merge( df_unmatched, df_right, how='left', on=[ADDR] )
    df_result, df_unmatched = isolate_unmatched( df_merge, left_columns, df_result, util.PERMIT_NUMBER )

    # Expand addresses in permit data
    df_right = util.expand_address_ranges( df_right )

    # Merge on expanded addresses
    df_merge = pd.merge( df_unmatched, df_right, how='left', on=[ADDR] )
    df_result, df_unmatched = isolate_unmatched( df_merge, left_columns, df_result, util.PERMIT_NUMBER )

    # Expand addresses in project data
    df_unmatched = util.expand_address_ranges( df_unmatched )

    # Merge on expanded addresses
    df_merge = pd.merge( df_unmatched, df_right, how='left', on=[ADDR] )
    df_result, df_unmatched = isolate_unmatched( df_merge, left_columns, df_result, util.PERMIT_NUMBER )

    return df_result


# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Combine weatherization tables' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve tables from database
    df_jobs = pd.read_sql_table( 'GlcacJobs_L', engine, index_col=util.ID, parse_dates=True )
    df_permits = pd.read_sql_table( 'BuildingPermits_L_Wx', engine, index_col=util.ID, parse_dates=True )

    # Use normalized addresses to correlate projects with permits
    df_result = combine_tables( df_jobs, df_permits )

    df_job_permit_map = df_result[[util.JOB_NUMBER, util.PERMIT_NUMBER]]
    df_jobs = pd.merge( df_jobs, df_job_permit_map, how='left', on=util.JOB_NUMBER )

    # Create tables in database
    util.create_table( 'GlcacJobsWithPermits_L', conn, cur, df=df_jobs )

    util.report_elapsed_time()
