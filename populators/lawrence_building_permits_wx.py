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


# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Clean weatherization data' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve raw weatherization permit tables - official, past, and latest
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
    df_permits = util.merge_with_assessment_data( df_permits, engine=engine, sort_by=[util.ACCOUNT_NUMBER] )

    # Clean up
    df_permits = df_permits.dropna( axis='columns', how='all' )
    df_permits = df_permits.drop_duplicates( subset=[util.PERMIT_NUMBER], keep='first' )
    df_permits = df_permits.sort_values( by=[util.PERMIT_NUMBER] )

    # Create table in database
    util.create_table( 'BuildingPermits_L_Wx', conn, cur, df=df_permits )

    util.report_elapsed_time()
