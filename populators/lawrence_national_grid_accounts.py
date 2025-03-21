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


# Clean up garbage in address column
def clean_address_column( df, col ):
    df[col] = df[col].fillna( '' )
    df[col] = df[col].str.upper()
    df[col] = df[col].str.replace( '*', '', regex=False )
    df[col] = df[col].str.replace( '\s+', ' ', regex=True )
    df[col] = df[col].str.strip()

    return df


##################################

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Generate tablea of National Grid accounts and meters' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )


    # Retrieve raw basic service table from database
    df_bs = pd.read_sql_table( 'RawNgAccountsBs_L', engine, index_col=util.ID, parse_dates=True )
    df_bs[util.ACCOUNT] = df_bs[util.ACCOUNT].fillna(0).astype( 'int64' )

    # Extract residential accounts
    df_bs = df_bs[ df_bs[util.SERVICE_DESCRIPTION].str.contains( 'Residential' ) ].copy()

    # Clean up address fields
    addr_cols = [util.SERV_ADDR_1, util.SERV_ADDR_2, util.SERV_ADDR_3, util.SERV_ADDR_4]
    for addr_col in addr_cols:
        df_bs = clean_address_column( df_bs, addr_col )

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df_bs[ADDR] = df_bs[util.SERV_ADDR_1] + ' ' + df_bs[util.SERV_ADDR_2] + ' ' + df_bs[util.SERV_ADDR_3] + ' ' + df_bs[util.SERV_ADDR_4]
    df_bs[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df_bs.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Save table
    df_bs.reset_index( drop=True )
    util.create_table( 'RawNgAccountsBs_L', conn, cur, df=df_bs )


    # Retrieve raw third-party supplier table from database
    df_tps = pd.read_sql_table( 'RawNgAccountsTps_L', engine, index_col=util.ID, parse_dates=True )

    # Extract residential accounts
    df_tps = df_tps[ df_tps[util.SERVICE_DESCRIPTION].str.contains( 'Residential' ) ].copy()

    # Clean up address column
    df_tps = clean_address_column( df_tps, util.SERVICE_ADDRESS )
    df_tps[util.SERVICE_ADDRESS] = df_tps[util.SERVICE_ADDRESS].str.replace( ' LAWRENCE MA \d+$', '', regex=True )

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df_tps[ADDR] = df_tps[util.SERVICE_ADDRESS]
    df_tps[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df_tps.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Save table
    df_tps.reset_index( drop=True )
    util.create_table( 'NgAccountsTps_L', conn, cur, df=df_tps )


    # Create table of Basic Service R2 accounts
    df_r2_bs = df_bs[df_bs[util.SERVICE_CODE] == 'R2A'].copy()
    df_r2_bs[util.THIRD_PARTY_SUPPLY] = util.NO
    df_r2_bs[util.RATEPAYER_ID] = df_r2_bs[util.ACCOUNT]

    # Create table of TPS R2 accounts
    df_r2_tps = df_tps[df_tps[util.RATE_CODE] == 'R-2'].copy()
    df_r2_tps[util.THIRD_PARTY_SUPPLY] = util.YES
    df_r2_tps[util.RATEPAYER_ID] = df_r2_tps[util.BARCODE]

    # Create table of all R2 accounts
    df_r2 = pd.concat( [df_r2_bs, df_r2_tps], ignore_index=True )
    df_r2 = df_r2.drop_duplicates( subset=[util.RATEPAYER_ID] )
    df_r2.reset_index( drop=True )

    # Merge meters dataframe with assessment data
    table_name = 'NgAccountsR2_L'
    df_r2 = util.merge_with_assessment_data( table_name, df_r2, engine=engine, sort_by=util.COLUMN_ORDER[table_name], drop_subset=util.COLUMN_ORDER[table_name] )

    # Save table
    util.create_table( table_name, conn, cur, df=df_r2 )

    util.report_elapsed_time()
