# Copyright 2022 Energize Lawrence.  All rights reserved.

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

    # Retrieve assessment tables from database
    commercial_columns = \
    [
        ADDR,
        util.ACCOUNT_NUMBER,
        util.HEATING_FUEL,
        util.HEATING_FUEL + util._DESC,
        util.HEATING_TYPE,
        util.HEATING_TYPE + util._DESC,
        util.AC_TYPE,
        util.AC_TYPE + util._DESC,
    ]
    not_in_residential = \
    [
        util.HEATING_FUEL + util._DESC,
        util.AC_TYPE + util._DESC,
    ]
    residential_columns = list( set( commercial_columns ) - set( not_in_residential ) )
    df_assessment_com = pd.read_sql_table( 'Assessment_L_Commercial', engine, index_col=util.ID, columns=commercial_columns, parse_dates=True )
    df_assessment_res = pd.read_sql_table( 'Assessment_L_Residential', engine, index_col=util.ID, columns=residential_columns, parse_dates=True )

    # Merge left dataframe with assessment data
    df_result = util.merge_with_assessment_data( df_left, df_assessment_com, df_assessment_res, [util.PERMIT_NUMBER, util.ACCOUNT_NUMBER] )

    # Create table in database
    util.create_table( 'BuildingPermits_L_Solar', conn, cur, df=df_result )

    util.report_elapsed_time()
