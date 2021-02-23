# Copyright 2021 Energize Andover.  All rights reserved.

##############################################
# System requirements for reading PDF files
#
# pip install tabula-py
# Download and install java JRE
#
##############################################

import argparse
import os
import tabula

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util



SKIPROWS_MAX = 5

DROP_COLUMNS = \
[
    'Status',
    'Source',
    'LicExpDate',
    'PaidAmount',
]

REQUIRED_COLUMNS = [ util.PERMIT_FOR, util.DATE_ISSUED, util.PARCEL_ID ]

######################


# Clean raw file input
def clean_table( df ):

    # Skip leading rows until we find a valid header
    skiprows = 0
    while ( df.columns[0] != util.PIN ) and ( skiprows < SKIPROWS_MAX ):
        new_header = df.iloc[0]
        df = df[1:]
        df.columns = new_header

    # If we found a valid header, clean the dataframe
    if skiprows < SKIPROWS_MAX:
        df = clean_df( df )
    else:
        df = None

    return df


# Clean dataframe extracted from raw input
def clean_df( df ):

    # Drop empty columns and rows
    df = df.dropna( axis='columns', how='all' )
    df = df.dropna( axis='rows', how='all' )

    # Clean column names
    df.columns = df.columns.fillna( '' ).astype( str )
    df.columns = df.columns.str.replace( '\r', '' )
    df.columns = df.columns.str.replace( ' ', '' )

    # Strip unwanted characters from specific columns
    df[util.PIN] = df[util.PIN].str.replace( '\r', '' )
    df[util.PERMITFOR] = df[util.PERMITFOR].str.replace( '\r', '' )
    df[util.TOTALFEE] = df[util.TOTALFEE].replace( '[\$,]', '', regex=True ).astype( float ).astype( int )
    cost_col = util.PROJECTCOST if util.PROJECTCOST in df.columns else util.COST
    df[cost_col] = df[cost_col].replace( '[\$,]', '', regex=True ).astype( float ).astype( int )

    # Drop unwanted columns
    drop_columns = []
    for col in DROP_COLUMNS:
        if col in df.columns:
            drop_columns.append( col )
    df = df.drop( columns=drop_columns )

    # Prepare for database
    df = util.prepare_for_database( df, 'BuildingPermits' )

    # Drop rows that do not have specific required fields
    df = df.dropna( axis='rows', how='all', subset=REQUIRED_COLUMNS )

    return df


######################

# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Process town data' )
    parser.add_argument( '-d', dest='input_directory',  help='Input directory - Location of PDF and MS Excel file(s)' )
    parser.add_argument( '-o', dest='output_filename',  help='Output filename - Name of SQLite database file', required=True )
    parser.add_argument( '-t', dest='output_table_name',  help='Output table name - Name of target table in SQLite database file', required=True )
    parser.add_argument( '-c', dest='create', action='store_true', help='Create new database?' )
    args = parser.parse_args()

    # Initialize empty result
    df_result = None

    # Process all files in specified directory
    for filename in os.listdir( args.input_directory ):

        # Get next input file
        input_path = args.input_directory + '/' + filename
        print( '\n"{0}"'.format( input_path ) )
        extension = os.path.splitext(filename)[1]

        # Read input file
        if extension == '.xlsx':
            df = pd.read_excel( input_path, dtype=object )
        elif extension == '.pdf':
            df, = tabula.read_pdf( input_path, pages='all', multiple_tables=False, lattice=True, )
        else:
            df = None
            print( 'Unexpected file type:', extension )

        # If we got input, clean it
        if df is not None:
            df = clean_table( df )
        else:
            print( 'Could not clean table' )

        # If we got a clean table, append it to the result
        if df is not None:

            if df_result is None:
                df_result = df
            else:
                df_result = df_result.append( df )

    # Sort result table on date
    df_result = df_result.sort_values( by=[util.DATE_ISSUED] )

    # Open output file
    conn, cur, engine = util.open_database( args.output_filename, args.create )

    # Save result to database
    util.create_table( args.output_table_name, conn, cur, df=df_result )

    # Report elapsed time
    util.report_elapsed_time()
