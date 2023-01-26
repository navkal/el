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
    parser = argparse.ArgumentParser( description='Correlate business entries with assessment data' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve raw Businesses table from database
    df_bus_1 = pd.read_sql_table( 'RawBusinesses_1', engine, index_col=util.ID, parse_dates=True )
    df_bus_2 = pd.read_sql_table( 'RawBusinesses_2', engine, index_col=util.ID, parse_dates=True, columns=[util.LICENSE_NUMBER, util.BUSINESS_MANAGER] )
    df_left = pd.merge( df_bus_1, df_bus_2, how='left', on=util.LICENSE_NUMBER )

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df_left[ADDR] = df_left[util.LOCATION]
    df_left[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df_left.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Read parcels assessment data and select columns for merge
    df_parcels = util.read_parcels_table_for_merge( engine, columns=None )
    df_parcels = df_parcels[ [ADDR] + list( df_parcels[df_parcels.columns.difference( df_left.columns )] ) ]

    # Merge left dataframe with assessment data
    table_name = 'Businesses_L'
    df_result = util.merge_with_assessment_data( table_name, df_left, df_parcels=df_parcels, sort_by=[util.LICENSE_NUMBER, util.ACCOUNT_NUMBER] )

    # Save final table of businesses
    util.create_table( table_name, conn, cur, df=df_result )

    util.report_elapsed_time()
