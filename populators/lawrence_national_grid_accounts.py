# Copyright 2025 Energize Lawrence.  All rights reserved.

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


RATE_CLASS_KEEP_COLUMNS = \
[
    util.RATEPAYER_ID,
    util.THIRD_PARTY_SUPPLY,
    util.SERVICE_NAME,
    ADDR,
    STREET_NUMBER,
    STREET_NAME,
    OCCUPANCY,
    ADDITIONAL,
]


# Clean up garbage in address column
def clean_address_column( df, col ):
    df[col] = df[col].fillna( '' )
    df[col] = df[col].str.upper()
    df[col] = df[col].str.replace( '*', '', regex=False )
    df[col] = df[col].str.replace( '\s+', ' ', regex=True )
    df[col] = df[col].str.strip()

    return df


# Create a table of National Grid accounts selected by rate class
def create_rate_class_table( s_rate_class, df_bs, df_tps ):

    # Create table of Basic Service accounts
    df_rate_class_bs = df_bs[df_bs[util.SERVICE_CODE] == 'R' + s_rate_class + 'A'].copy()
    df_rate_class_bs[util.THIRD_PARTY_SUPPLY] = util.NO
    df_rate_class_bs[util.RATEPAYER_ID] = df_rate_class_bs[util.ACCOUNT]

    # Create table of TPS accounts
    df_rate_class_tps = df_tps[df_tps[util.RATE_CODE] == 'R-1'].copy()
    df_rate_class_tps[util.THIRD_PARTY_SUPPLY] = util.YES
    df_rate_class_tps[util.RATEPAYER_ID] = df_rate_class_tps[util.BARCODE]

    # Create table of all accounts
    df_rate_class = pd.concat( [df_rate_class_bs, df_rate_class_tps], ignore_index=True )
    df_rate_class = df_rate_class[RATE_CLASS_KEEP_COLUMNS]
    df_rate_class = df_rate_class.drop_duplicates( subset=[util.RATEPAYER_ID] )
    df_rate_class.reset_index( drop=True )

    # Merge meters dataframe with assessment data
    table_name = 'NgAccountsR' + s_rate_class + '_L'
    df_rate_class = util.merge_with_assessment_data( table_name, df_rate_class, engine=engine, sort_by=util.COLUMN_ORDER[table_name], drop_subset=util.COLUMN_ORDER[table_name] )

    # Save table
    util.create_table( table_name, conn, cur, df=df_rate_class )

    return




##################################

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Generate tablea of National Grid accounts and meters' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Read table mapping street names misspelled by National Grid to correct spellings
    df_street_names = pd.read_sql_table( 'RawNgStreetNames_L', engine, index_col=util.ID )
    dc_street_names = dict( zip( df_street_names[util.BAD_SPELLING], df_street_names[util.GOOD_SPELLING] ) )


    # Retrieve raw basic service table from database
    df_bs = pd.read_sql_table( 'RawNgAccountsBasic_L', engine, index_col=util.ID, parse_dates=True )
    df_bs[util.ACCOUNT] = df_bs[util.ACCOUNT].fillna(0).astype( 'int64' )

    # Edit misspelled street names
    df_bs[util.SERV_ADDR_2] = df_bs[util.SERV_ADDR_2].replace( dc_street_names, regex=True )

    # Save table
    util.create_table( 'NgAccountsBasic_L', conn, cur, df=df_bs )


    # Extract residential accounts
    df_bs = df_bs[ df_bs[util.SERVICE_DESCRIPTION].str.contains( 'Residential' ) ].copy()

    # Clean up address fields
    addr_cols = [util.SERV_ADDR_1, util.SERV_ADDR_2, util.SERV_ADDR_3, util.SERV_ADDR_4]
    for addr_col in addr_cols:
        df_bs = clean_address_column( df_bs, addr_col )

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df_bs[ADDR] = df_bs[util.SERV_ADDR_1] + ' ' + df_bs[util.SERV_ADDR_2] + ' ' + df_bs[util.SERV_ADDR_3] + ' ' + df_bs[util.SERV_ADDR_4]
    df_bs[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df_bs.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )


    # Retrieve raw third-party supplier table from database
    df_tps = pd.read_sql_table( 'RawNgAccountsTps_L', engine, index_col=util.ID, parse_dates=True )

    # Clean up address column
    df_tps = clean_address_column( df_tps, util.SERVICE_ADDRESS )
    df_tps[util.SERVICE_ADDRESS] = df_tps[util.SERVICE_ADDRESS].str.replace( ' LAWRENCE MA \d+$', '', regex=True )

    # Edit misspelled street names
    df_tps[util.SERVICE_ADDRESS] = df_tps[util.SERVICE_ADDRESS].replace( dc_street_names, regex=True )

    # Save table
    df_tps.reset_index( drop=True )
    util.create_table( 'NgAccountsTps_L', conn, cur, df=df_tps )

    # Extract residential accounts
    df_tps = df_tps[ df_tps[util.SERVICE_DESCRIPTION].str.contains( 'Residential' ) ].copy()

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df_tps[ADDR] = df_tps[util.SERVICE_ADDRESS]
    df_tps[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df_tps.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )


    # Build and save table R1 accounts
    create_rate_class_table( '1', df_bs, df_tps )

    # Build and save table R2 accounts
    create_rate_class_table( '2', df_bs, df_tps )


    util.report_elapsed_time()
