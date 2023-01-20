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

    # Retrieve assessment tables for merging
    assessment_columns = \
    [
        ADDR,
        util.ACCOUNT_NUMBER,
    ]
    df_assessment_com = pd.read_sql_table( 'Assessment_L_Commercial', engine, index_col=util.ID, columns=assessment_columns, parse_dates=True )
    df_assessment_res = pd.read_sql_table( 'Assessment_L_Residential', engine, index_col=util.ID, columns=assessment_columns, parse_dates=True )


    #
    # Clean/enhance the GLCAC jobs data
    #

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
    df_jobs = util.merge_with_assessment_data( df_jobs, df_assessment_com, df_assessment_res, [util.ACCOUNT_NUMBER] )
    df_jobs[ADDR] = df_jobs[TEMP]
    df_jobs = df_jobs.drop( columns=[TEMP] )

    # Clean up
    df_jobs = df_jobs.dropna( axis='columns', how='all' )
    df_jobs = df_jobs.drop_duplicates( subset=[util.JOB_NUMBER], keep='first' )
    df_jobs = df_jobs.sort_values( by=[util.JOB_NUMBER] )

    # Create table in database
    util.create_table( 'GlcacJobs_L', conn, cur, df=df_jobs )


    #
    # Clean/enhance weatherization permit data
    #

    # Retrieve city permits tables - official, past, and latest - from database
    df_permits = pd.read_sql_table( 'RawBuildingPermits_Wx', engine, index_col=util.ID, parse_dates=True )
    df_past = pd.read_sql_table( 'RawBuildingPermits_Wx_Past', engine, index_col=util.ID, parse_dates=True )
    df_2023 = pd.read_sql_table( 'RawBuildingPermits_Wx_2023', engine, index_col=util.ID, parse_dates=True )

    # Combine official and past permits in one dataframe, preserving source information
    df_permits = util.combine_dataframes( df_permits, df_past, [util.PERMIT_NUMBER], 'first', [util.PERMIT_NUMBER] )
    df_permits = util.combine_dataframes( df_permits, df_2023, [util.PERMIT_NUMBER], 'last', [util.PERMIT_NUMBER] )

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df_permits[ADDR] = df_permits[util.ADDRESS].str.strip()
    df_permits[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df_permits.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Merge permits dataframe with assessment data
    df_permits = util.merge_with_assessment_data( df_permits, df_assessment_com, df_assessment_res, [util.ACCOUNT_NUMBER] )

    # Clean up
    df_permits = df_permits.dropna( axis='columns', how='all' )
    df_permits = df_permits.drop_duplicates( subset=[util.PERMIT_NUMBER], keep='first' )
    df_permits = df_permits.sort_values( by=[util.PERMIT_NUMBER] )

    # Create table in database
    util.create_table( 'BuildingPermits_L_Wx', conn, cur, df=df_permits )


    util.report_elapsed_time()
