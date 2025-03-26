# Copyright 2025 Energize Andover.  All rights reserved.

######################
#
# Extracts columns from table in specified database
# and converts them to rendering data in specified KML file.
#
# Data structures governing behavior of this script are in
# parcels_to_kml_mapy.py
#
# Required parameters:
# -i input database filename
# -c output CSV filename
# -k output KML filename
#
# Example:
# -i lawrence_master.sqlite -c parcels_to_kml.csv -k parcels_to_kml.kml
#
######################

import argparse
import os

import pandas as pd
pd.options.display.width = 0

import sqlite3
import sqlalchemy

import parcels_to_kml_map as map
TABLE = map.TABLE
COLUMNS = map.COLUMNS
MAP = map.MAP

import parcels_to_kml_util as util


######################

# Open the SQLite database
def open_database():

    print( '\nOpening database: ' + args.input_filename )

    print( ' Connecting...' )
    conn = sqlite3.connect( args.input_filename )
    cur = conn.cursor()

    engine = sqlalchemy.create_engine( 'sqlite:///' + args.input_filename )

    return conn, cur, engine

######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate Excel file from SQLite database' )
    parser.add_argument( '-i', dest='input_filename', help='Input filename - Name of SQLite database file', required=True )
    parser.add_argument( '-c', dest='csv_filename', help='Output filename - Name of CSV file', required=True )
    parser.add_argument( '-k', dest='kml_filename', help='Output filename - Name of KML file', required=True )
    args = parser.parse_args()

    print( '' )

    # Check input file extensions
    ext = os.path.splitext( args.input_filename )[1]
    if ext != '.sqlite':
        print( f'Input database file extension "{ext}" is not supported.  Please use ".sqlite".  ' )
        exit()

    ext = os.path.splitext( args.csv_filename )[1]
    if ext != '.csv':
        print( f'Output CSV file extension "{ext}" is not supported.  Please use ".csv".  ' )
        exit()

    ext = os.path.splitext( args.kml_filename )[1]
    if ext != '.kml':
        print( f'Output KML file extension "{ext}" is not supported.  Please use ".kml".  ' )
        exit()

    # Report arguments
    print( 'Arguments' )
    print( '  Input database:', args.input_filename )
    print( '  Output CSV:', args.csv_filename )
    print( '  Output KML:', args.kml_filename )

    # Open the input database
    conn, cur, engine = open_database()

    # Read the parcels table
    print( '' )
    print( f'Reading columns from {TABLE} table:' )
    for c in COLUMNS:
        print( f'  {c}' )

    try:
        df = pd.read_sql_table( TABLE, engine, columns=COLUMNS )
    except Exception as e:
        print( '' )
        print( f'Error reading {TABLE}: {e}' )
        exit()

    # Edit Vision hyperlinks encoded for Excel
    pattern = r'=HYPERLINK\("(http.*pid=\d+).*'
    df = df.replace( to_replace=pattern, value=r'\1', regex=True )

    # Sort
    df = df.sort_values( by=COLUMNS )
    df = df.reset_index( drop=True )

    # Replace column values with visual attributes
    for col in MAP:
        if col in df.columns:
            df[col] = df[col].replace( MAP[col] )

    # Save to CSV
    print( '' )
    print( f'Writing to {args.csv_filename}' )
    df.to_csv( args.csv_filename, index=False, header=True )

    # Convert CSV to KML
    util.create_kml( args.csv_filename, args.kml_filename )

    print( '' )
    print( 'Done' )
