# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import re

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
    parser = argparse.ArgumentParser( description='Generate Solar Building Permits table' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve table from database
    df_left = pd.read_sql_table( 'RawBuildingPermits_Solar', engine, index_col=util.ID, parse_dates=True )

    # Clean text suffixes out of numeric column
    df_left[util.WATTS_PER_MODULE] = df_left[util.WATTS_PER_MODULE].str.extract( r'([0-9]+\.?[0-9]*|\.[0-9]+)' )
    df_left[util.WATTS_PER_MODULE] = df_left[util.WATTS_PER_MODULE].astype( float )

    # Extract kW metric from work description
    df_left[util.KW_DC] = df_left[util.WORK_DESCRIPTION].str.extract( r'([0-9]+\.?[0-9]*|\.[0-9]+) *kw(?:[^h]|$)', flags=re.IGNORECASE )
    df_left[util.KW_DC] = df_left[util.KW_DC].astype( float )

    # Backfill kW metric with calculated value
    idx = df_left.loc[ df_left[util.KW_DC].isnull() & ( df_left[util.WATTS_PER_MODULE] > 100 ) ].index
    df_left.at[idx,util.KW_DC] = ( df_left.loc[idx][util.MODULES] * df_left.loc[idx][util.WATTS_PER_MODULE] ) / 1000

    # Backfill kW metric again, with copied value
    idx = df_left.loc[ df_left[util.KW_DC].isnull() ].index
    df_left.at[idx,util.KW_DC] = df_left.loc[idx][util.WATTS_PER_MODULE]

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df_left[ADDR] = df_left[util.ADDRESS]
    df_left[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df_left.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Merge left dataframe with assessment data
    table_name = 'BuildingPermits_L_Solar'
    df_result = util.merge_with_assessment_data( table_name, df_left, engine=engine, sort_by=[util.PERMIT_NUMBER, util.FILE_NUMBER, util.DATE_DUE_FOR_INSPECTION] )

    # Create table in database
    util.create_table( table_name, conn, cur, df=df_result )

    # Create summary table in database
    df_result = df_result.drop_duplicates( subset=[util.PERMIT_NUMBER], keep='last' )
    util.create_table( table_name + '_Summary', conn, cur, df=df_result )

    util.report_elapsed_time()
