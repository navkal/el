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
    parser = argparse.ArgumentParser( description='Generate Columbia Gas Building Permits table' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve table from database
    df = pd.read_sql_table( 'RawBuildingPermits_Cga', engine, index_col=util.ID, parse_dates=True )

    # Prepare to normalize
    df[ADDR] = df[util.ADDR_STREET_NUMBER].str.strip() + ' ' + df[util.ADDR_STREET_NAME].str.strip()
    df[ADDR] = df[ADDR].str.replace( r" ST ST ", " ST ", regex=True ).str.strip()
    df[ADDR] = df[ADDR].str.replace( r" T$", " ST", regex=True ).str.strip()
    df[ADDR] = df[ADDR].str.replace( r" APT FRT ", " ", regex=True ).str.strip()

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY]] = df.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Create table in database
    util.create_table( 'BuildingPermits_L_Cga', conn, cur, df=df )

    util.report_elapsed_time()
