# Copyright 2022 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

#------------------------------------------------------

if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Clean up raw energy usage table from MassSave' )
    parser.add_argument( '-d', dest='db_filename',  help='Database filename' )
    parser.add_argument( '-i', dest='input_table',  help='Input table name' )
    parser.add_argument( '-o', dest='output_table',  help='Output table name' )
    parser.add_argument( '-r', dest='remove_columns',  help='Remove columns' )
    parser.add_argument( '-n', dest='numeric_columns',  help='Numeric columns' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read table from database
    df_raw = pd.read_sql_table( args.input_table, engine, index_col=util.ID, parse_dates=True )

    # Create copy of dataframe
    df = df_raw.copy()

    # Drop unwanted columns
    if args.remove_columns:
        df = df.drop( columns=args.remove_columns.split( ',' ) )

    # Drop rows with empty cells
    df = df.dropna( axis='rows', how='any' )

    # Drop rows reporting totals
    df = df[ df[util.TOWN_NAME] != 'All Towns' ]

    # Clean up numeric columns
    col_names = args.numeric_columns.split( ',' )

    for col in col_names:

        # Remove dollar signs
        df[col] = df[col].replace( '\$', '', regex=True )

        # Remove commas
        df[col] = df[col].replace( ',', '', regex=True )

        # Strip spaces
        df[col] = df[col].str.strip()

        # Drop rows with non-numeric values
        df = df.drop( axis='rows', index=df[ ~df[col].str.isdigit() ].index )

        # Set numeric datatype
        df[col] = df[col].astype(int)

    # Save result to database
    util.create_table( args.output_table, conn, cur, df=df )

    # Report elapsed time
    util.report_elapsed_time()
