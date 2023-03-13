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
    parser = argparse.ArgumentParser( description='Generate Census table' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve table from database
    df = pd.read_sql_table( 'RawCensus_L', engine, index_col=util.ID, parse_dates=True )

    # Prepare address fragments for normalization
    df[util.RADDR_STREET_NUMBER] = df[util.RADDR_STREET_NUMBER].fillna(0).astype(int).astype(str)
    df[util.RADDR_STREET_NUMBER_SUFFIX] = df[util.RADDR_STREET_NUMBER_SUFFIX].fillna('').astype(str)
    df[util.RADDR_STREET_NAME] = df[util.RADDR_STREET_NAME].fillna('').astype(str)
    df[util.RADDR_APARTMENT_NUMBER] = df[util.RADDR_APARTMENT_NUMBER].fillna('').astype(str)

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df[ADDR] = df[util.RADDR_STREET_NUMBER] + df[util.RADDR_STREET_NUMBER_SUFFIX] + ' ' + df[util.RADDR_STREET_NAME] + ' ' + df[util.RADDR_APARTMENT_NUMBER]
    df[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Merge census dataframe with assessment data
    table_name = 'Census_L'
    df = util.merge_with_assessment_data( table_name, df, engine=engine, sort_by=[util.RESIDENT_ID, util.ACCOUNT_NUMBER], drop_subset=[util.RESIDENT_ID] )

    # Drop older entries that appear to represent the same person
    df[util.CENSUS_YEAR] = df[util.CENSUS_YEAR].fillna( 0 ).astype( int )
    df = df.sort_values( by=[util.CENSUS_YEAR] )
    df = df.drop_duplicates( subset=[util.FIRST_NAME, util.LAST_NAME, util.DATE_OF_BIRTH], keep='last' )
    df = df.sort_values( by=[util.RESIDENT_ID, util.ACCOUNT_NUMBER] )

    # Clean up phone numbers
    df[util.PHONE] = df[util.PHONE].fillna( '' ).astype( str ).replace( '\.0$', '', regex=True )
    idx_ten_digits = df[df[util.PHONE].str.len() == 10].index
    sr_phone = df.loc[idx_ten_digits, util.PHONE]
    df.at[idx_ten_digits, util.PHONE] = sr_phone.str[:3] + '-' + sr_phone.str[3:6] + '-' + sr_phone.str[6:]

    # Calculate age
    df[util.AGE] = df[util.DATE_OF_BIRTH].apply( lambda date_of_birth: util.calculate_age( date_of_birth ) )

    # Save table in database
    util.create_table( table_name, conn, cur, df=df )

    util.report_elapsed_time()
