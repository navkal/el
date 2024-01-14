# Copyright 2022 Energize Andover.  All rights reserved.

import argparse

import chardet

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import warnings

import sys
sys.path.append( '../util' )
import util


######################

# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Generate database table from Excel spreadsheet' )
    parser.add_argument( '-b', dest='do_this_before_starting',  help='Initialization code to execute before doing anything else' )
    parser.add_argument( '-i', dest='input_filename',  help='Input filename - Name of MS Excel file' )
    parser.add_argument( '-d', dest='input_directory',  help='Input directory - Location of MS Excel file(s)' )
    parser.add_argument( '-v', dest='csv', action='store_true',  help='Read input as CSV' )
    parser.add_argument( '-f', dest='field_separator', help='Field separator in CSV input file' )
    parser.add_argument( '-l', dest='column_labels',  help='Labels of additional columns, to be populated by fragments of filename, delimited by underscore' )
    parser.add_argument( '-e', dest='keep_unnamed', action='store_true', help='Keep unnamed columns?'  )
    parser.add_argument( '-y', dest='hyperlinks', action='store_true', help='Preserve hyperlinks?'  )
    parser.add_argument( '-r', dest='skip_rows', type=int, help='Number of leading rows to skip' )
    parser.add_argument( '-m', dest='drop_empty', action='store_true', help='Drop all empty columns and rows?'  )
    parser.add_argument( '-n', dest='dropna_subset',  help='Column subset to be considered in dropna() operation' )
    parser.add_argument( '-p', dest='drop_columns',  help='Comma-separated list of column labels to drop' )
    parser.add_argument( '-k', dest='keep_columns',  help='Comma-separated list of column labels to keep' )
    parser.add_argument( '-u', dest='uppercase_columns',  help='Comma-separated list of column labels for conversion to uppercase' )
    parser.add_argument( '-s', dest='sort_columns',  help='Comma-separated list of column labels to be used as basis for sort' )
    parser.add_argument( '-x', dest='exclude_unmapped', action='store_true', help='Exclude unmapped column names?'  )
    parser.add_argument( '-z', dest='synthesize_unmapped', action='store_true', help='Synthesize unmapped column names?'  )
    parser.add_argument( '-o', dest='output_filename',  help='Output filename - Name of SQLite database file', required=True )
    parser.add_argument( '-t', dest='output_table_name',  help='Output table name - Name of target table in SQLite database file', required=True )
    parser.add_argument( '-c', dest='create', action='store_true', help='Create new database?' )
    args = parser.parse_args()

    # Optionally execute initialization code
    if args.do_this_before_starting != None:
        exec( args.do_this_before_starting )

    skiprows = range( args.skip_rows ) if ( args.skip_rows != None ) else None

    # Determine field separator for CSV format.  Default is comma.
    csv_sep = bytes( args.field_separator, 'utf-8' ).decode( 'unicode_escape' ) if args.field_separator != None else ','

    # Read input
    if args.input_directory != None:

        # Read multiple input files
        df_xl = util.read_excel_files( args.input_directory, args.column_labels, skiprows, args.csv, csv_sep )

    elif args.input_filename != None:

        # Read single input file

        if args.csv:
            # Extract dataframe from a delimited text file

            # Detect encoding of input file
            with open( args.input_filename, 'rb' ) as rawdata:
                encoding_info = chardet.detect( rawdata.read( 10000 ) )

            # Get the dataframe from the input file
            df_xl = pd.read_csv( args.input_filename, encoding=encoding_info['encoding'], sep=csv_sep, dtype=object, skiprows=skiprows )

        elif args.hyperlinks:
            # Get dataframe with hyperlinks
            df_xl = util.read_excel_with_hyperlinks( args.input_filename, skiprows )
        else:
            # Get dataframe without hyperlinks
            warnings.filterwarnings( 'ignore', category=UserWarning, module='openpyxl' )
            df_xl = pd.read_excel( args.input_filename, dtype=object, skiprows=skiprows )

    else:
        # Error: No input specified
        exit( 'Input filename or directory + prefix required' )

    # Remove excess whitespace in column heads
    df_xl.columns = df_xl.columns.str.replace( '\s+', ' ', regex=True ).str.strip()

    # Optionally drop all empty columns and rows
    if args.drop_empty:
        df_xl = df_xl.dropna( axis='columns', how='all' )
        df_xl = df_xl.dropna( axis='rows', how='all' )

    # Drop unwanted rows
    if args.dropna_subset != None:
        df_xl = df_xl.dropna( subset=args.dropna_subset.split( ',' ) )

    # Convert values in specified columns to uppercase
    if args.uppercase_columns != None:
        uppercase_columns = args.uppercase_columns.split( ',' )
        for col in uppercase_columns:
            df_xl[col] = df_xl[col].fillna('').astype( str ).str.upper()

    # Sort based on specified column values
    if args.sort_columns != None:
        sort_columns = args.sort_columns.split( ',' )
        for col in sort_columns:
            df_xl[col] = df_xl[col].fillna('').astype( str )
        df_xl = df_xl.sort_values( by=sort_columns )

    # Optionally drop unnamed columns
    if not args.keep_unnamed:
        cols = df_xl.columns
        for col in cols:
            if col.startswith( 'Unnamed: ' ):
                df_xl = df_xl.drop( columns=[col] )

    # Drop unwanted columns
    if args.drop_columns != None:
        df_xl = df_xl.drop( columns=args.drop_columns.split( ',' ) )

    # Keep only specified columns
    if args.keep_columns != None:
        df_xl = df_xl[ args.keep_columns.split( ',' ) ]

    # Prepare data for saving to database
    df_xl = util.prepare_for_database( df_xl, args.output_table_name, exclude_unmapped=args.exclude_unmapped, synthesize_unmapped=args.synthesize_unmapped )

    # Open output file
    conn, cur, engine = util.open_database( args.output_filename, args.create )

    # Read pre-existing table data from output file
    try:
        df_db = pd.read_sql_table( args.output_table_name, engine, index_col=util.ID, parse_dates=util.get_date_columns( df_xl ) )
        all_cols = df_db.columns.union( df_xl.columns )
        df_db = df_db.reindex( columns=all_cols )
        df_xl = df_xl.reindex( columns=all_cols )
    except:
        df_db = pd.DataFrame()

    # Combine existing and new data
    df = df_db.append( df_xl )

    # Drop duplicates
    df = df.drop_duplicates( keep='last' )

    # Save result to database
    util.create_table( args.output_table_name, conn, cur, df=df )

    # Report elapsed time
    util.report_elapsed_time()
