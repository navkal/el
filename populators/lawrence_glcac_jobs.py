# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util
import normalize

ADDR = util.NORMALIZED_ADDRESS
STREET_NUMBER = util.NORMALIZED_STREET_NUMBER
STREET_NAME = util.NORMALIZED_STREET_NAME
OCCUPANCY = util.NORMALIZED_OCCUPANCY
ADDITIONAL = util.NORMALIZED_ADDITIONAL_INFO

TEMP = 'temp'


# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Clean weatherization data' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve GLCAC jobs table from database
    df_jobs = pd.read_sql_table( 'RawGlcacJobs', engine, index_col=util.ID, parse_dates=True )

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df_jobs[ADDR] = df_jobs[util.ADDRESS].str.strip()
    df_jobs[ADDR] = df_jobs[ADDR].str.upper()
    df_jobs[ADDR] = df_jobs[ADDR].str.split( ' MA ', expand=True )[0]
    df_jobs[ADDR] = df_jobs[ADDR].str.rsplit( n=1, expand=True )[0]
    df_jobs[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df_jobs.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Merge jobs dataframe with assessment data
    df_jobs[TEMP] = df_jobs[ADDR]
    df_jobs[ADDR] = df_jobs[STREET_NUMBER] + ' ' + df_jobs[STREET_NAME]
    df_jobs = util.merge_with_assessment_data( df_jobs, engine=engine, sort_by=[util.ACCOUNT_NUMBER] )
    df_jobs[ADDR] = df_jobs[TEMP]
    df_jobs = df_jobs.drop( columns=[TEMP] )

    # Clean up
    df_jobs = df_jobs.dropna( axis='columns', how='all' )
    df_jobs = df_jobs.drop_duplicates( subset=[util.JOB_NUMBER], keep='first' )
    df_jobs = df_jobs.sort_values( by=[util.JOB_NUMBER] )

    # Create table in database
    util.create_table( 'GlcacJobs_L', conn, cur, df=df_jobs )

    util.report_elapsed_time()
