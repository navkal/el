# Copyright 2024 Energize Andover.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import chardet

import sys
sys.path.append('../util')
import util


LAWRENCE_MIN = 250092501000
LAWRENCE_MAX = 250092518999

######################

# Main program
if __name__ == '__main__':

    # Retrieve arguments
    parser = argparse.ArgumentParser( description='Extract data on motor vehicles in Lawrence' )
    parser.add_argument( '-i', dest='input_filename',  help='Vehicle input csv filename' )
    parser.add_argument( '-o', dest='output_filename',  help='Vehicle output database filename' )
    args = parser.parse_args()

    # Detect encoding of input csv file
    with open( args.input_filename, 'rb' ) as rawdata:
        encoding_info = chardet.detect( rawdata.read( 10000 ) )

    # Get raw table from input csv file
    df = pd.read_csv( args.input_filename, encoding=encoding_info['encoding'], dtype=object )

    # Rename the columns
    df = util.rename_columns( df, 'RawMotorVehicles_L' )

    # Extract rows pertaining to Lawrence
    df[util.CENSUS_GEO_ID] = df[util.CENSUS_GEO_ID].fillna(0).astype( 'int64' )
    df = df[ ( df[util.CENSUS_GEO_ID] >= LAWRENCE_MIN ) & ( df[util.CENSUS_GEO_ID] <= LAWRENCE_MAX ) ]

    # Extract rows pertaining to the most recent year
    a_dates = []
    for idx, df_group in df.groupby( by=[util.DATE] ):
        a_dates.append( df_group.iloc[0][util.DATE] )
    a_dates.sort( reverse=True )
    df = df[ df[util.DATE] == a_dates[0] ]

    # Drop unwanted columns
    df = df.drop( columns=[util.DATE, util.MPO, util.TOWN_NAME] )

    # Clean up numeric columns
    df[util.COUNT] = df[util.COUNT].astype(int)
    df[util.DAILY_VMT] = df[util.DAILY_VMT].astype(float).round( decimals=6 )

    # Unpack census Geo ID
    df[util.CENSUS_TRACT] = df[util.CENSUS_GEO_ID].astype(str).str[5:9].astype(int)
    df[util.CENSUS_BLOCK_GROUP] = df[util.CENSUS_GEO_ID].astype(str).str[9:].astype(int)

    # Sort
    df = df.sort_values( by=[util.CENSUS_GEO_ID, util.VEHICLE_TYPE, util.ADVANCED_VEHICLE_TYPE] )

    # Save to master database
    conn, cur, engine = util.open_database( args.output_filename, True )
    util.create_table( 'MotorVehicles_L', conn, cur, df=df )

    # Report elapsed time
    util.report_elapsed_time()
