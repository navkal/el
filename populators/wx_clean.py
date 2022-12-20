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

PERMIT_SOURCE = 'permit_source'
OFFICIAL = 'official'
PAST = 'past'

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

    # Drop empty columns
    df_jobs = df_jobs.dropna( axis='columns', how='all' )

    # Create table in database
    util.create_table( 'GlcacJobs_L', conn, cur, df=df_jobs )

    # Retrieve city permits tables - official and past - from database
    df_permits = pd.read_sql_table( 'RawBuildingPermits_Wx', engine, index_col=util.ID, parse_dates=True )
    df_past = pd.read_sql_table( 'RawBuildingPermits_Past_Wx', engine, index_col=util.ID, parse_dates=True )

    # Combine official and past permits in one dataframe, preserving source information
    df_permits[PERMIT_SOURCE] = OFFICIAL
    df_past[PERMIT_SOURCE] = PAST
    df_permits = df_permits.append( df_past, ignore_index=True )
    df_permits = df_permits.drop_duplicates( subset=[util.PERMIT_NUMBER] )

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df_permits[ADDR] = df_permits[util.ADDRESS].str.strip()
    df_permits[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df_permits.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Drop empty columns
    df_permits = df_permits.dropna( axis='columns', how='all' )

    # Create table in database
    util.create_table( 'BuildingPermits_L_Wx', conn, cur, df=df_permits )

    util.report_elapsed_time()
