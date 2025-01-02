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

##################################

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Generate table of National Grid accounts' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve raw table from database
    df_accounts = pd.read_sql_table( 'RawNationalGridAccounts_L', engine, index_col=util.ID, parse_dates=True )

    # Extract residential accounts
    df_residential = df_accounts[ ( df_accounts[util.ACCOUNT_TYPE] == 'Residential' ) | ( df_accounts[util.ACCOUNT_TYPE] == 'Residential Assistance' ) ].copy()

    # Clean up address fields
    addr_cols = [util.SERV_ADDR_1, util.SERV_ADDR_2, util.SERV_ADDR_3, util.SERV_ADDR_4]
    for addr_col in addr_cols:
        df_residential[addr_col] = df_residential[addr_col].fillna( '' )
        df_residential[addr_col] = df_residential[addr_col].str.replace( '*', '', regex=False )
        df_residential[addr_col] = df_residential[addr_col].str.replace( '\s+', ' ', regex=True )
        df_residential[addr_col] = df_residential[addr_col].str.split(' LAWRENCE').str[0]
        df_residential[addr_col] = df_residential[addr_col].str.strip()

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df_residential[ADDR] = df_residential[util.SERV_ADDR_1] + ' ' + df_residential[util.SERV_ADDR_2] + ' ' + df_residential[util.SERV_ADDR_3] + ' ' + df_residential[util.SERV_ADDR_4]
    df_residential[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df_residential.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Save full residential account data for reference
    df_accounts = df_residential.copy()
    df_accounts[util.ACCOUNT_NUMBER] = df_accounts[util.ACCOUNT_NUMBER].fillna(0).astype( 'int64' )
    util.create_table( 'NationalGridAccounts_L', conn, cur, df=df_accounts )

    # Partition residential accounts as representing basic service or third-party supply
    df_basic_service = df_residential[ pd.notnull( df_residential[util.ACCOUNT_NUMBER] ) ]
    df_third_party = df_residential[ pd.isnull( df_residential[util.ACCOUNT_NUMBER] ) ]

    # Prepare to third-party records for correlation with basic service records by address
    df_third_party = df_third_party[[ADDR]]
    df_third_party[util.THIRD_PARTY_SUPPLY] = util.YES

    # Merge the dataframes on address
    df = pd.merge( df_basic_service, df_third_party, on=ADDR, how='left' )

    # Fill empty cells in third-party supply column
    df[util.THIRD_PARTY_SUPPLY] = df[util.THIRD_PARTY_SUPPLY].fillna( util.NO )

    # Prepare dataframe
    df = df.drop( columns=[util.BASIC_OR_TPS] )
    df = df.drop_duplicates()
    df = df.sort_values( util.COLUMN_ORDER['NationalGridMeters_L'] )
    df[util.ACCOUNT_NUMBER] = df[util.ACCOUNT_NUMBER].astype( 'int64' )

    # Write to the database
    util.create_table( 'NationalGridMeters_L', conn, cur, df=df )

    util.report_elapsed_time()
