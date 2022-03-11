# Copyright 2020 Energize Andover.  All rights reserved.

import argparse
import sqlite3
import sqlalchemy
from shutil import copyfile


# Open the SQLite database
def open_database( filename ):

    print( '\nOpening database: ' + filename )

    print( ' Connecting...' )
    conn = sqlite3.connect( filename )
    cur = conn.cursor()

    engine = sqlalchemy.create_engine( 'sqlite:///' + filename )

    return conn, cur, engine


def copy_views( input_filename, views_filename, output_filename ):

    # Create a copy of the input file
    copyfile( input_filename, output_filename )

    # Open databases
    vw_conn, vw_cur, vw_engine = open_database( views_filename )
    out_conn, out_cur, out_engine = open_database( output_filename )

    # Fetch views
    vw_cur.execute( 'SELECT sql FROM sqlite_master WHERE type="view"' )
    rows = vw_cur.fetchall()

    # Recreate views in output file
    print( '' )
    input_db = {}
    for row in rows:
        print( row[0] )
        out_cur.execute( row[0] )

    out_conn.commit


# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Correlate employees with residents' )
    parser.add_argument( '-i', dest='input_filename',  help='Input database filename' )
    parser.add_argument( '-v', dest='views_filename',  help='Filename of database containing views to be migrated' )
    parser.add_argument( '-o', dest='output_filename',  help='Output database filename' )
    args = parser.parse_args()

    copy_views( args.input_filename, args.views_filename, args.output_filename )
