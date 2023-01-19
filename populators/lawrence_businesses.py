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

    # Retrieve assessment tables from database
    commercial_columns = \
    [
        ADDR,
        util.ACCOUNT_NUMBER,
        util.OWNER_NAME,
        util.MADDR_LINE.format( 1 ),
        util.MADDR_CITY,
        util.MADDR_STATE,
        util.MADDR_ZIP_CODE,
        util.SALE_DATE,
        util.STORY_HEIGHT,
        util.RENTAL_LIVING_UNITS,
        util.ROOF_STRUCTURE,
        util.ROOF_STRUCTURE + util._DESC,
        util.ROOF_COVER,
        util.ROOF_COVER + util._DESC,
        util.HEATING_FUEL,
        util.HEATING_FUEL + util._DESC,
        util.HEATING_TYPE,
        util.HEATING_TYPE + util._DESC,
        util.AC_TYPE,
        util.AC_TYPE + util._DESC,
        util.TOTAL_ASSESSED_VALUE,
        util.LAND_USE_CODE,
        util.LAND_USE_CODE + '_1',
        util.LAND_USE_CODE + util._DESC,
    ]
    not_in_residential = \
    [
        util.RENTAL_LIVING_UNITS,
        util.HEATING_FUEL + util._DESC,
        util.AC_TYPE + util._DESC,
        util.LAND_USE_CODE + '_1',
        util.LAND_USE_CODE + util._DESC,
    ]
    residential_columns = list( set( commercial_columns ) - set( not_in_residential ) )
    df_assessment_com = pd.read_sql_table( 'Assessment_L_Commercial', engine, index_col=util.ID, columns=commercial_columns, parse_dates=True )
    df_assessment_res = pd.read_sql_table( 'Assessment_L_Residential', engine, index_col=util.ID, columns=residential_columns, parse_dates=True )

    # Merge left dataframe with assessment data
    df_result = util.merge_with_assessment_data( df_left, df_assessment_com, df_assessment_res, [util.LICENSE_NUMBER, util.ACCOUNT_NUMBER] )

    # Save final table of businesses
    util.create_table( 'Businesses_L', conn, cur, df=df_result )

    util.report_elapsed_time()
