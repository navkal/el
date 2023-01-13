# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Copy a table from input database to output database' )
    parser.add_argument( '-i', dest='input_filename',  help='Input database filename' )
    parser.add_argument( '-f', dest='from_table_name',  help='Name of source table', required=True )
    parser.add_argument( '-t', dest='to_table_name',  help='Name of destination table', required=True )
    parser.add_argument( '-o', dest='output_filename',  help='Output database filename' )
    args = parser.parse_args()

    # Open the input database
    conn, cur, engine = util.open_database( args.input_filename, False )

    # Retrieve table from input database
    df = pd.read_sql_table( args.from_table_name, engine, index_col=util.ID )

    # Open the output database
    conn, cur, engine = util.open_database( args.output_filename, False )

    # Save table to output database
    util.create_table( args.to_table_name, conn, cur, df=df )

    util.report_elapsed_time()
