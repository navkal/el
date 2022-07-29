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

PERMIT_PREFIX = 'Permit #:'

def fix_permit_number( pn ):
    pn = re.sub( '\\u00A0', ' ', pn ).replace( PERMIT_PREFIX, '' ).strip()
    return pn

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Generate Sunrun Building Permits table' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve table from database
    df_raw = pd.read_sql_table( 'RawBuildingPermits_Sunrun', engine, index_col=util.ID, parse_dates=True )

    # Initialize empty dataframe and series
    df_left = pd.DataFrame()
    sr_row = pd.Series( dtype=object )

    for index, row in df_raw.iterrows():

        if row[util.STATUS] != None:

            # Starting new row.  Save values collected from previous row in dataframe and reinitialize.
            if len( sr_row ) > 0:
                df_left = df_left.append( sr_row, ignore_index=True )
                sr_row = pd.Series( dtype=object )

            # Unscramble values that occur in row that supposedly contains status column
            sr_row[util.FILE_NUMBER] = row[util.STATUS]
            sr_row[util.FILE_NUMBER_LINK] = row[util.STATUS + '_1']
            sr_row[util.STATUS] = row[util.ADDRESS]
            sr_row[util.ADDRESS] = row[util.OPENED]
            sr_row[util.CLOSED_DATE] = row[util.CLOSED_DATE]

        elif row[util.ADDRESS] != None:

            # Unscramble values that occur in row that supposedly contains address column

            if row[util.ADDRESS + '_1'] != None:
                sr_row[util.PERMIT_NUMBER] = fix_permit_number( row[util.ADDRESS] )
                sr_row[util.PERMIT_NUMBER_LINK] = row[util.ADDRESS + '_1']

            elif row[util.ADDRESS].startswith( PERMIT_PREFIX ):
                sr_row[util.PERMIT_NUMBER] = fix_permit_number( row[util.ADDRESS] )

            else:
                sr_row[util.DESCRIPTION] = row[util.ADDRESS]

    # Append the last row
    df_left = df_left.append( sr_row, ignore_index=True )

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
    df_result = util.merge_with_assessment_data( df_left, df_assessment_com, df_assessment_res, [util.PERMIT_NUMBER, util.FILE_NUMBER, util.ACCOUNT_NUMBER] )

    # Create table in database
    util.create_table( 'BuildingPermits_L_Sunrun', conn, cur, df=df_result )

    util.report_elapsed_time()
