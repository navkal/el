# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util
import normalize
import vision

ADDR = util.NORMALIZED_ADDRESS
STREET_NUMBER = util.NORMALIZED_STREET_NUMBER
STREET_NAME = util.NORMALIZED_STREET_NAME
OCCUPANCY = util.NORMALIZED_OCCUPANCY
ADDITIONAL = util.NORMALIZED_ADDITIONAL_INFO


# Create database table documenting column name mappings
def document_column_names( df ):

    # Invert column name mappings
    map_1 = { v: k for k, v in util.CONSISTENT_COLUMN_NAMES['RawResidential_1'].items() }
    map_2 = { v: k for k, v in util.CONSISTENT_COLUMN_NAMES['RawResidential_2'].items() }
    map_3 = { v: k for k, v in util.CONSISTENT_COLUMN_NAMES['RawResidential_3'].items() }
    map_4 = { v: k for k, v in util.CONSISTENT_COLUMN_NAMES['RawResidential_4'].items() }
    map_5 = { v: k for k, v in util.CONSISTENT_COLUMN_NAMES['RawResidential_5'].items() }

    # Build matrix mapping each database column name to its origins
    names = []
    for col in df.columns:
        row = \
        [
            col,
            map_1[col] if col in map_1 else None,
            map_2[col] if col in map_2 else None,
            map_3[col] if col in map_3 else None,
            map_4[col] if col in map_4 else None,
            map_5[col] if col in map_5 else None,
        ]
        names.append( row )

    # Create dataframe
    df_names = pd.DataFrame( columns = ['database', 'drop_7', 'drop_6', 'drop_2', 'csv_concat', 'csv_merge'], data=names )

    # Save in database
    util.create_table( 'Residential_ColumnNames', conn, cur, df=df_names )


# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Correlate data in residential tables' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve tables from database
    df_1 = pd.read_sql_table( 'RawResidential_1', engine, index_col=util.ID, parse_dates=True )
    df_2 = pd.read_sql_table( 'RawResidential_2', engine, index_col=util.ID, parse_dates=True )
    df_3 = pd.read_sql_table( 'RawResidential_3', engine, index_col=util.ID, parse_dates=True )
    df_4 = pd.read_sql_table( 'RawResidential_4', engine, index_col=util.ID, parse_dates=True )
    df_5 = pd.read_sql_table( 'RawResidential_5', engine, index_col=util.ID, parse_dates=True )

    # For duplicated account numbers, retain only last entry
    df_1 = df_1.drop_duplicates( subset=[util.ACCOUNT_NUMBER], keep='last' )
    df_2 = df_2.drop_duplicates( subset=[util.ACCOUNT_NUMBER], keep='last' )
    df_3 = df_3.drop_duplicates( subset=[util.ACCOUNT_NUMBER], keep='last' )
    df_4 = df_4.drop_duplicates( subset=[util.ACCOUNT_NUMBER], keep='last' )
    df_5 = df_5.drop_duplicates( subset=[util.ACCOUNT_NUMBER], keep='last' )

    # Merge df_1, df_2, and df_3 on account number
    columns_to_merge = [util.ACCOUNT_NUMBER] + list( df_2.columns.difference( df_1.columns ) )
    df_merge = pd.merge( df_1, df_2[columns_to_merge], how='left', on=[util.ACCOUNT_NUMBER] )
    columns_to_merge = [util.ACCOUNT_NUMBER] + list( df_3.columns.difference( df_merge.columns ) )
    df_merge = pd.merge( df_merge, df_3[columns_to_merge], how='left', on=[util.ACCOUNT_NUMBER] )

    # Reconcile address representation in df_4
    df_4[util.LADDR_STREET_NUMBER] = df_4[util.LADDR_STREET_NUMBER].fillna( '' )
    df_4[util.LADDR_STREET_NAME] = df_4[util.LADDR_STREET_NAME].fillna( '' )
    df_4[util.LOCATION] = df_4[util.LADDR_STREET_NUMBER] + ' ' + df_4[util.LADDR_STREET_NAME]
    df_4 = df_4.drop( columns=[util.LADDR_STREET_NUMBER, util.LADDR_STREET_NAME] )

    # Concatenate df_4 to merged dataframe and drop duplicates
    df_merge = pd.concat( [df_merge, df_4] )
    df_merge = df_merge.drop_duplicates( subset=[util.ACCOUNT_NUMBER], keep='first' )

    # Merge df_5 on account number
    columns_to_merge = [util.ACCOUNT_NUMBER] + list( df_5.columns.difference( df_merge.columns ) )
    df_merge = pd.merge( df_merge, df_5[columns_to_merge], how='left', on=[util.ACCOUNT_NUMBER] )

    # Drop empty columns
    df_merge = df_merge.dropna( how='all', axis=1 )

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df_merge[ADDR] = df_merge[util.LOCATION]
    df_merge[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df_merge.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Incorporate scraped data from online Vision database
    df_result = vision.incorporate_vision_assessment_data( engine, df_merge )

    # Document column names in database
    document_column_names( df_result )

    # Save final table of residential assessments
    util.create_table( 'Assessment_L_Residential_Merged', conn, cur, df=df_result )

    util.report_elapsed_time()
