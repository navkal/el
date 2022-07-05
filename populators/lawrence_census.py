# Copyright 2022 Energize Lawrence.  All rights reserved.

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


# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Generate Census table' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve table from database
    df = pd.read_sql_table( 'RawCensus', engine, index_col=util.ID, parse_dates=True )

    # Prepare address fragments for normalization
    df[util.RADDR_STREET_NUMBER] = df[util.RADDR_STREET_NUMBER].fillna(0).astype(int).astype(str)
    df[util.RADDR_STREET_NUMBER_SUFFIX] = df[util.RADDR_STREET_NUMBER_SUFFIX].fillna('').astype(str)
    df[util.RADDR_STREET_NAME] = df[util.RADDR_STREET_NAME].fillna('').astype(str)
    df[util.RADDR_APARTMENT_NUMBER] = df[util.RADDR_APARTMENT_NUMBER].fillna('').astype(str)

    # Complain about timestamps in address fragments
    # df_complain = df[ df[util.RADDR_STREET_NUMBER_SUFFIX].str.contains( ':' ) | df[util.RADDR_APARTMENT_NUMBER].str.contains( ':' )]
    # df_complain.to_excel( '../test/census_address_defects.xlsx', index=False )
    # exit()

    # Clean up address fragments that (inexplicably) contain date or time values
    df.loc[ df[util.RADDR_STREET_NUMBER_SUFFIX].str.contains( ':' ), util.RADDR_STREET_NUMBER_SUFFIX ] = ''
    df.loc[ df[util.RADDR_APARTMENT_NUMBER].str.contains( ':' ), util.RADDR_APARTMENT_NUMBER ] = ''

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df[ADDR] = df[util.RADDR_STREET_NUMBER] + df[util.RADDR_STREET_NUMBER_SUFFIX] + ' ' + df[util.RADDR_STREET_NAME] + ' ' + df[util.RADDR_APARTMENT_NUMBER]
    df[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY]] = df.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Overwrite table in database
    util.create_table( 'LawrenceCensus', conn, cur, df=df )

    util.report_elapsed_time()
