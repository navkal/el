# Copyright 2019 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
import numpy as np
import sqlite3


######################
#
# Sample parameter sequences
#
# Default behavior:
# -i example.xlsx -o example.sqlite -t Example
#
# Convert floating point columns to int:
# -i xl2db_int.xlsx -o xl2db_int.sqlite -t Example -n
#
######################


# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Load Excel sheet into SQLite database' )
    parser.add_argument( '-i', dest='input_filename',  help='Input filename - Name of MS Excel file', required=True )
    parser.add_argument( '-o', dest='output_filename',  help='Output filename - Name of SQLite database file', required=True )
    parser.add_argument( '-t', dest='output_table_name',  help='Output table name - Name of target table in SQLite database file', required=True )
    parser.add_argument( '-n', dest='float_to_int',  help='Convert float to int?', action='store_true' )
    args = parser.parse_args()

    # Read the spreadsheet and clean up column labels
    df = pd.read_excel( args.input_filename )
    df.columns = df.columns.astype( str )
    df.columns = df.columns.str.replace( ' ', '' )

    # Convert float columns to integer
    if args.float_to_int:
        for col_name in df.columns:
            if df[col_name].dtype == float:
                try:
                    df[col_name] = df[col_name].round().astype( pd.Int64Dtype() )
                except:
                    print( "Column '{}': Could not convert datatype from float to int".format( col_name ) )

    # Report
    print( 'Input filename:', args.input_filename )
    print( 'Output filename:', args.output_filename )
    print( 'Output table name:', args.output_table_name )
    print( 'Columns:', df.columns.tolist() )

    # Open the database
    conn = sqlite3.connect( args.output_filename )
    cur = conn.cursor()

    # Drop table if it already exists
    cur.execute( 'DROP TABLE IF EXISTS ' + args.output_table_name )

    # Generate SQL command to create table
    create_sql = 'CREATE TABLE ' + args.output_table_name + ' ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE'
    for col_name in df.columns:
        create_sql += ', "{0}"'.format( col_name )
    create_sql += ' )'

    # Create the empty table
    cur.execute( create_sql )

    # Load data into table
    df.to_sql( args.output_table_name, conn, if_exists='append', index=False )

    # Commit changes
    conn.commit()
