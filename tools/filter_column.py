# Copyright 2023 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
import sqlite3
import sqlalchemy


######################
#
# Sample parameter sequence
#
# -d db.sqlite -t TableName -c column_name -f filter_string -o OutputTableName
#
######################

FILTER_TABLE_SUFFIX = '_Filter'

# Open the SQLite database
def open_database():

    print( '\nOpening database: ' + args.database_filename )

    print( ' Connecting...' )
    conn = sqlite3.connect( args.database_filename )
    cur = conn.cursor()

    engine = sqlalchemy.create_engine( 'sqlite:///' + args.database_filename )

    return conn, cur, engine


# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Filter values in specified column' )
    parser.add_argument( '-d', dest='database_filename',  help='Name of SQLite database file', required=True )
    parser.add_argument( '-t', dest='input_table_name',  help='Input table name - Name of table in specified database', required=True )
    parser.add_argument( '-c', dest='input_column_name',  help='Input column name - Name of column in specified table', required=True )
    parser.add_argument( '-f', dest='filter_string',  help='Filter to apply to specified column', required=True )
    parser.add_argument( '-o', dest='output_table_name',  help='Output table name - Name of result table', required=True )
    args = parser.parse_args()

    # Report
    print( "Database '{}'".format( args.database_filename ) )
    print( "Table '{}'".format( args.input_table_name ) )
    print( "Column '{}'".format( args.input_column_name ) )
    print( "Filtering on '{}'".format( args.filter_string ) )

    # Open the input database
    conn, cur, engine = open_database()

    # Read the table
    df = pd.read_sql_table( args.input_table_name, engine, parse_dates=True )
    print( '' )

    if args.input_column_name not in df.columns:
        print( "Error: Column '{}' not found in table '{}'".format( args.input_column_name, args.input_table_name ) )
        exit()

    # Create filtered table
    df_filter = df[ df[args.input_column_name].str.contains( args.filter_string, regex=False ) ]
    df_filter = df_filter.drop( ['id'], axis=1 )

    print( "Saving results to table '{}'".format( args.output_table_name ) )

    # Drop table if it already exists
    cur.execute( 'DROP TABLE IF EXISTS ' + args.output_table_name )

    # Generate SQL command to create table
    create_sql = 'CREATE TABLE ' + args.output_table_name + ' ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE'
    for col_name in df_filter.columns:
        create_sql += ', "{0}"'.format( col_name )
    create_sql += ' )'

    # Create the empty table
    cur.execute( create_sql )

    # Load data into table
    df_filter.to_sql( args.output_table_name, conn, if_exists='append', index=False )

    # Commit changes
    conn.commit()
