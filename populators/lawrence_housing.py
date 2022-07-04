# Copyright 2022 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util
import normalize

ADDR = util.NORMALIZED_ADDRESS
STREET_NUMBER = util.NORMALIZED_STREET_NUMBER
STREET_NAME = util.NORMALIZED_STREET_NAME
OCCUPANCY = util.NORMALIZED_OCCUPANCY


# Create database table documenting column name mappings
def document_column_names():

    # Invert column name mappings
    map_1 = { v: k for k, v in util.CONSISTENT_COLUMN_NAMES['Housing_1'].items() }
    map_2 = { v: k for k, v in util.CONSISTENT_COLUMN_NAMES['Housing_2'].items() }
    map_3 = { v: k for k, v in util.CONSISTENT_COLUMN_NAMES['Housing_3'].items() }

    # Build matrix mapping each database column name to its origins
    names = []
    for col in df_merge.columns:
        row = \
        [
            col,
            map_1[col] if col in map_1 else None,
            map_2[col] if col in map_2 else None,
            map_3[col] if col in map_3 else None
        ]
        names.append( row )

    # Create dataframe
    df_names = pd.DataFrame( columns = ['database', 'drop_7', 'drop_6', 'drop_2'], data=names )

    # Save in database
    util.create_table( 'Housing_ColumnNames', conn, cur, df=df_names )


# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Correlate data in housing tables' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve tables from database
    df_1 = pd.read_sql_table( 'Housing_1', engine, index_col=util.ID, parse_dates=True )
    df_2 = pd.read_sql_table( 'Housing_2', engine, index_col=util.ID, parse_dates=True )
    df_3 = pd.read_sql_table( 'Housing_3', engine, index_col=util.ID, parse_dates=True )

    # For duplicated account numbers, retain only last entry
    df_1 = df_1.drop_duplicates( subset=[util.ACCOUNT_NUMBER], keep='last' )
    df_2 = df_2.drop_duplicates( subset=[util.ACCOUNT_NUMBER], keep='last' )
    df_3 = df_3.drop_duplicates( subset=[util.ACCOUNT_NUMBER], keep='last' )

    # Merge on account number
    columns_to_merge = [util.ACCOUNT_NUMBER] + list( df_2.columns.difference( df_1.columns ) )
    df_merge = pd.merge( df_1, df_2[columns_to_merge], how='left', on=[util.ACCOUNT_NUMBER] )
    columns_to_merge = [util.ACCOUNT_NUMBER] + list( df_3.columns.difference( df_merge.columns ) )
    df_merge = pd.merge( df_merge, df_3[columns_to_merge], how='left', on=[util.ACCOUNT_NUMBER] )

    # Sort on account number
    df_merge = df_merge.sort_values( by=[util.ACCOUNT_NUMBER] )

    # Drop empty columns
    df_merge = df_merge.dropna( how='all', axis=1 )

    # Remove parenthesized text from addresses
    df_merge[ADDR] = df_merge[util.LOCATION].str.replace( r"\(.*\)?", "", regex=True ).str.strip()

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df_merge[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY]] = df_merge.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Document column names in database
    document_column_names()

    # Save final table of housing assessments
    util.create_table( 'Assessment_Housing', conn, cur, df=df_merge )

    util.report_elapsed_time()
