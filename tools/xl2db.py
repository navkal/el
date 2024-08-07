# Copyright 2019 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
import sqlite3

import warnings
warnings.filterwarnings( 'ignore', category=UserWarning, module='pandas' )

######################
#
# Sample parameter sequences
#
# Default behavior:
# -i example.xlsx -o example.sqlite -t Example
#
# Select specific sheet by name:
# -i xl2db_sheet.xlsx -s "Sheet Name" -o xl2db_sheet.sqlite -t Example
#
# Convert floating point columns to int:
# -i xl2db_int.xlsx -o xl2db_int.sqlite -t Example -n
#
# Skip leading rows of input sheet:
# -i xl2db_int.xlsx -o xl2db_int.sqlite -t Example -r <number_of_rows>
#
######################


# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Load Excel sheet into SQLite database' )
    parser.add_argument( '-i', dest='input_filename',  help='Input filename - Name of MS Excel file', required=True )
    parser.add_argument( '-s', dest='sheet_name',  help='Input spreadsheet' )
    parser.add_argument( '-o', dest='output_filename',  help='Output filename - Name of SQLite database file', required=True )
    parser.add_argument( '-t', dest='output_table_name',  help='Output table name - Name of target table in SQLite database file', required=True )
    parser.add_argument( '-n', dest='float_to_int',  help='Convert float to int?', action='store_true' )
    parser.add_argument( '-r', dest='skip_rows', type=int, help='Number of leading rows to skip' )
    args = parser.parse_args()

    skiprows = range( args.skip_rows ) if ( args.skip_rows != None ) else None

    # Read the Excel sheet
    if args.sheet_name != None:
        # Specific sheet
        xl_doc = pd.ExcelFile( args.input_filename )
        df = xl_doc.parse( args.sheet_name, skiprows=skiprows )
    else:
        # Default sheet
        df = pd.read_excel( args.input_filename, skiprows=skiprows )

    # Clean up column labels
    df.columns = df.columns.astype( str )
    df.columns = df.columns.str.replace( '\s+', ' ', regex=True ).str.strip()

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
