# Copyright 2024 Energize Andover.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append('../util')
import util


LAWRENCE_MIN = util.LAWRENCE_MIN_GEO_ID
LAWRENCE_MAX = util.LAWRENCE_MAX_GEO_ID

######################

# Main program
if __name__ == '__main__':

    # Retrieve arguments
    parser = argparse.ArgumentParser( description='Extract US Census EJSCREEN data pertinent to Lawrence' )
    parser.add_argument( '-i', dest='input_filename',  help='EJSCREEN input csv filename' )
    parser.add_argument( '-o', dest='output_filename',  help='EJSCREEN output database filename' )
    args = parser.parse_args()

    # Get raw table from input csv file
    df = pd.read_csv( args.input_filename, encoding='latin1', dtype=object )

    # Rename global ID column
    df = df.rename( columns={ 'ID': util.CENSUS_GEO_ID } )

    # Extract rows pertaining to Lawrence
    df[util.CENSUS_GEO_ID] = df[util.CENSUS_GEO_ID].fillna(0).astype( 'int64' )
    df = df[ ( df[util.CENSUS_GEO_ID] >= LAWRENCE_MIN ) & ( df[util.CENSUS_GEO_ID] <= LAWRENCE_MAX ) ]

    # Sort
    df = df.sort_values( by=[util.CENSUS_GEO_ID] )

    # Save to output database
    conn, cur, engine = util.open_database( args.output_filename, True )
    util.create_table( 'Ejscreen_L', conn, cur, df=df )

    # Report elapsed time
    util.report_elapsed_time()
