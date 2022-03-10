# Copyright 2022 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util


# When any cell in a group is protected, set all cells in the group to 0
def zero_protected_groups( df, col_names ):

    p = args.protection_text
    c = col_names

    for index, row in df.iterrows():

        # First group - electric data
        if ( row[c[0]] == p ) or ( row[c[1]] == p ) or ( row[c[2]] == p ) :
            df.at[index, c[0]] = '0'
            df.at[index, c[1]] = '0'
            df.at[index, c[2]] = '0'

        # Second group - gas data
        if ( row[c[3]] == p ) or ( row[c[4]] == p ) or ( row[c[5]] == p ) :
            df.at[index, c[3]] = '0'
            df.at[index, c[4]] = '0'
            df.at[index, c[5]] = '0'

    # If all cells in both electric and gas data are 0, drop the row
    drop_index = df[ (df[c[0]]=='0') & (df[c[1]]=='0') & (df[c[2]]=='0') & (df[c[3]]=='0') & (df[c[4]]=='0') & (df[c[5]]=='0') ].index
    df = df.drop( drop_index )

    return df


#------------------------------------------------------

if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Clean up raw energy usage table from MassSave' )
    parser.add_argument( '-d', dest='db_filename', help='Database filename' )
    parser.add_argument( '-i', dest='input_table', help='Input table name' )
    parser.add_argument( '-o', dest='output_table', help='Output table name' )
    parser.add_argument( '-r', dest='remove_columns', help='Remove columns' )
    parser.add_argument( '-n', dest='numeric_columns', help='Numeric columns' )
    parser.add_argument( '-z', dest='zero_values', help='Values to replace with zero' )
    parser.add_argument( '-p', dest='protection_text', help='Text string used to protect a non-public numeric value' )
    parser.add_argument( '-x', dest='drop_input_table', action='store_true', help='Drop input table?' )
    parser.add_argument( '-c', dest='complement_table',  help='Name of optional table containing complement of refined table?' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read table from database
    df_raw = pd.read_sql_table( args.input_table, engine, index_col=util.ID )

    # Create copy of dataframe
    df = df_raw.copy()

    # Drop unwanted columns
    if args.remove_columns:
        df = df.drop( columns=args.remove_columns.split( ',' ) )

    # Drop rows with empty cells
    df = df.dropna( axis='rows', how='any' )

    # Drop rows reporting totals
    if util.TOWN_NAME in df.columns:
        df = df[ df[util.TOWN_NAME] != 'All Towns' ]

    # Save copy as basis for generating complement table
    df_org = df.copy()

    # Replace text with 0
    if args.zero_values:
        zero_values = args.zero_values.split( ',' )
        for zero_value in zero_values:
            df = df.replace( zero_value, '0' )

    # Clean up numeric columns
    if args.numeric_columns:
        col_names = args.numeric_columns.split( ',' )

        # Handle cell groups containing protected values
        if args.protection_text:
            df = zero_protected_groups( df, col_names )

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

    # Optionally drop input table
    if args.drop_input_table:
        cur.execute( 'DROP TABLE IF EXISTS ' + args.input_table )

    # Save result to database
    util.create_table( args.output_table, conn, cur, df=df )

    # Optionally save complement of result table to database
    if args.complement_table:
        df_comp = df_org[~df_org.index.isin(df.index)]
        util.create_table( args.complement_table, conn, cur, df=df_comp )


    # Report elapsed time
    util.report_elapsed_time()
