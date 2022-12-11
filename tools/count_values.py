# Copyright 2019 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
import sqlite3
import sqlalchemy


######################
#
# Sample parameter sequence
#
# -d db.sqlite -t TableName -c column_name
#
######################

COUNT_COLUMN_NAME = 'count'
COUNT_TABLE_SUFFIX = '_Count'

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
    parser = argparse.ArgumentParser( description='Count distinct values in specified column' )
    parser.add_argument( '-d', dest='database_filename',  help='Name of SQLite database file', required=True )
    parser.add_argument( '-t', dest='input_table_name',  help='Input table name - Name of table in specified database', required=True )
    parser.add_argument( '-c', dest='input_column_name',  help='Input column name - Name of column in specified table', required=True )
    args = parser.parse_args()

    # Report
    print( 'Counting distinct values in:' )
    print( "  Database '{}'".format( args.database_filename ) )
    print( "  Table '{}'".format( args.input_table_name ) )
    print( "  Column '{}'".format( args.input_column_name ) )

    # Open the input database
    conn, cur, engine = open_database()

    # Read the table
    df = pd.read_sql_table( args.input_table_name, engine, parse_dates=True )
    print( '' )

    if args.input_column_name not in df.columns:
        print( "Error: Column '{}' not found in table '{}'".format( args.input_column_name, args.input_table_name ) )
        exit()

    # Count values in specified column, sort, and convert to dataframe
    df_count = df[args.input_column_name].value_counts().sort_index().to_frame()

    # Fix column names in result table
    df_count = df_count.reset_index()
    df_count = df_count.rename( columns={ df_count.columns[0]: df_count.columns[1], df_count.columns[1]: COUNT_COLUMN_NAME } )

    # Generate output table name
    table_part = '_'.join( args.input_table_name.split() )
    column_part = '_'.join( args.input_column_name.split() )
    output_table_name = table_part + '_' +  column_part + COUNT_TABLE_SUFFIX
    print( "Saving results to table '{}'".format( output_table_name ) )

    # Drop table if it already exists
    cur.execute( 'DROP TABLE IF EXISTS ' + output_table_name )

    # Generate SQL command to create table
    create_sql = 'CREATE TABLE ' + output_table_name + ' ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE'
    for col_name in df_count.columns:
        create_sql += ', "{0}"'.format( col_name )
    create_sql += ' )'

    # Create the empty table
    cur.execute( create_sql )

    # Load data into table
    df_count.to_sql( output_table_name, conn, if_exists='append', index=False )

    # Commit changes
    conn.commit()
