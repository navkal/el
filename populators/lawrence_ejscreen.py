# Copyright 2024 Energize Andover.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append('../util')
import util


LAWRENCE_MIN = util.LAWRENCE_MIN_GEO_ID
LAWRENCE_MAX = util.LAWRENCE_MAX_GEO_ID

######################

# Main program
if __name__ == '__main__':

    # Retrieve arguments
    parser = argparse.ArgumentParser( description='Extract US Census EJSCREEN data pertinent to Lawrence' )
    parser.add_argument( '-i', dest='input_filename',  help='EJSCREEN input - csv filename' )
    parser.add_argument( '-d', dest='doc_filename',  help='EJSCREEN documentation - xlsx filename' )
    parser.add_argument( '-p', dest='drop_filename',  help='List of columns to drop - xlsx filename' )
    parser.add_argument( '-o', dest='output_filename',  help='EJSCREEN output - database filename' )
    args = parser.parse_args()

    # Get raw table from input csv file
    df = pd.read_csv( args.input_filename, encoding='latin1', dtype=object )

    # Rename global ID column
    df = df.rename( columns={ 'ID': util.CENSUS_GEO_ID } )

    # Extract rows pertaining to Lawrence
    df[util.CENSUS_GEO_ID] = df[util.CENSUS_GEO_ID].fillna(0).astype( 'int64' )
    df = df[ ( df[util.CENSUS_GEO_ID] >= LAWRENCE_MIN ) & ( df[util.CENSUS_GEO_ID] <= LAWRENCE_MAX ) ]

    # Sort
    df = df.sort_values( by=[util.CENSUS_GEO_ID] )

    # Save data to output database
    conn, cur, engine = util.open_database( args.output_filename, True )
    util.create_table( 'Ejscreen_L', conn, cur, df=df )

    # Read list of dropped columns and prepare for merge
    df_drop = pd.read_excel( args.drop_filename )
    df_drop = df_drop.rename( columns={ 'id': 'gdb_fieldname', 'Drop (Y/N)': 'dropped' } )
    df_drop[df_drop.columns] = df_drop.apply( lambda x: x.str.strip() )

    # Save documentation in database
    xl_doc = pd.ExcelFile( args.doc_filename )
    for sheet_name in xl_doc.sheet_names:

        # Read the Excel sheet
        df_sheet = xl_doc.parse( sheet_name, skiprows=1 )

        # Strip spaces
        string_cols = df_sheet.select_dtypes( object ).columns
        df_sheet[string_cols] = df_sheet[string_cols].apply( lambda x: x.str.strip() )

        # Rename the columns
        for col_name in df_sheet.columns:
            fixed_name = '_'.join( col_name.split() ).lower()
            df_sheet = df_sheet.rename( columns={ col_name: fixed_name } )

        # Rename the table
        table_name = ''.join( sheet_name.split() )

        # Merge list of dropped columns
        if table_name == 'StatePercentilesDataset':
            df_sheet = pd.merge( df_sheet, df_drop, on=['gdb_fieldname'], how='left' )
            df_sheet['dropped'] = df_sheet['dropped'].fillna( '' )

        # Save the table to the database
        util.create_table( table_name, conn, cur, df=df_sheet )

    # Report elapsed time
    util.report_elapsed_time()
