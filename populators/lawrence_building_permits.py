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
    parser = argparse.ArgumentParser( description='Generate Building Permits table' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    parser.add_argument( '-p', dest='permit_type',  help='Permit type fragment in the raw table name' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    if args.permit_type is not None:
        suffix = '_' + args.permit_type
    else:
        suffix = ''

    # Retrieve table from database
    df_left = pd.read_sql_table( 'RawBuildingPermits' + suffix, engine, index_col=util.ID, parse_dates=True )

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df_left[ADDR] = df_left[util.ADDRESS]
    df_left[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df_left.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Merge left dataframe with assessment data
    df_result = util.merge_with_assessment_data( df_left, engine=engine )

    # Create table in database
    util.create_table( 'BuildingPermits_L' + suffix, conn, cur, df=df_result )

    util.report_elapsed_time()
