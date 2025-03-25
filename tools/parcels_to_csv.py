# Copyright 2025 Energize Andover.  All rights reserved.

import argparse
import os

import pandas as pd
pd.options.display.width = 0

import sqlite3
import sqlalchemy

PARCELS_TABLE = 'Assessment_L_Parcels'

COLUMNS_DEFAULT = \
[
    'latitude',
    'longitude',
    'heating_fuel_description',
    'heating_type_description',
    'ward_number',
    'precinct_number',
    'vision_link',
]


# Open the SQLite database
def open_database():

    print( '\nOpening database: ' + args.input_filename )

    print( ' Connecting...' )
    conn = sqlite3.connect( args.input_filename )
    cur = conn.cursor()

    engine = sqlalchemy.create_engine( 'sqlite:///' + args.input_filename )

    return conn, cur, engine



# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate Excel file from SQLite database' )
    parser.add_argument( '-i', dest='input_filename', help='Input filename - Name of SQLite database file', required=True )
    parser.add_argument( '-o', dest='output_filename', help='Output filename - Name of CSV file', required=True )
    parser.add_argument( '-c', dest='columns', default=','.join(COLUMNS_DEFAULT), help='Comma-separated list of column labels to include in output' )

    args = parser.parse_args()


    input_ext = os.path.splitext( args.input_filename )[1]
    if input_ext != '.sqlite':
        print( 'Input file extension "{}" is not supported.  Please use ".sqlite".  '.format( input_ext ) )
        exit()

    output_ext = os.path.splitext( args.output_filename )[1]
    if output_ext != '.csv':
        print( 'Input file extension "{}" is not supported.  Please use ".csv".  '.format( output_ext ) )
        exit()

    # Get list of requested columns
    ls_columns = args.columns.split( ',' )

    # Report
    print( 'Input filename:', args.input_filename )
    print( 'Output filename:', args.output_filename )
    print( 'Columns:' )
    for c in ls_columns:
        print( f'  {c}' )

    # Open the input database
    conn, cur, engine = open_database()

    # Read the parcels table
    print( '' )
    print( f'Reading {PARCELS_TABLE}' )
    try:
        df = pd.read_sql_table( PARCELS_TABLE, engine, columns=ls_columns )
    except Exception as e:
        print( '' )
        print( f'Error reading {PARCELS_TABLE}: {e}' )
        exit()

    # Edit Vision hyperlinks encoded for Excel
    pattern = r'=HYPERLINK\("(http.*pid=\d+).*'
    df = df.replace( to_replace=pattern, value=r'\1', regex=True )

    print( '' )
    print( f'Writing to {args.output_filename}' )
    df.to_csv( args.output_filename, index=False, header=True )

    print( '' )
    print( 'Done' )
