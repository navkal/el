# Copyright 2022 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option('display.max_rows', 500)
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

#------------------------------------------------------

if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Clean up raw energy usage table from MassSave' )
    parser.add_argument( '-d', dest='db_filename', help='Database filename' )
    parser.add_argument( '-i', dest='input_table', help='Input table name' )
    parser.add_argument( '-e', dest='input_table_electric', help='Name of table containing external electricity suppliers' )
    parser.add_argument( '-g', dest='input_table_gas', help='Name of table containing external gas suppliers' )
    parser.add_argument( '-o', dest='output_table', help='Output table name' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read raw input table from database
    df_raw = pd.read_sql_table( args.input_table, engine, index_col=util.ID )

    # Read optional table of external electricity suppliers from database
    if args.input_table_electric is not None:
        df_es = pd.read_sql_table( args.input_table_electric, engine, columns=[util.ACCOUNT_NUMBER, util.LOCATION_ADDRESS] )
    else:
        df_es = pd.DataFrame( columns=[util.ACCOUNT_NUMBER] )

    # Read optional table of external gas suppliers from database
    if args.input_table_gas is not None:

        # Read the table
        df_gas = pd.read_sql_table( args.input_table_gas, engine, columns=[util.UTILITY_ACCOUNT, util.STREET, util.CITY, util.STATE, util.ZIP] )

        # Extract account number
        df_gas[util.ACCOUNT_NUMBER] = df_gas[util.UTILITY_ACCOUNT].str.split( '-', expand=True )[0]

        # Concatenate address fragments into single field
        df_gas[util.LOCATION_ADDRESS] = df_gas[util.STREET] + ' ' + df_gas[util.CITY] + ' ' + df_gas[util.STATE] + ' ' + df_gas[util.ZIP]
        df_gas = df_gas[ [util.ACCOUNT_NUMBER, util.LOCATION_ADDRESS] ]

        # Combine all external suppliers into single dataframe
        df_es = df_es.append( df_gas, ignore_index=True )
        df_es[util.ACCOUNT_NUMBER] = df_es[util.ACCOUNT_NUMBER].astype(str)
        df_es = df_es.drop_duplicates()

    # Create copy of dataframe
    df = df_raw.copy()

    # Merge optional external supplier flag into dataframe
    if len( df_es ):
        df_es[util.EXTERNAL_SUPPLIER] = util.YES
        df = pd.merge( df, df_es, how='left', on=[util.ACCOUNT_NUMBER] )
        df[util.EXTERNAL_SUPPLIER] = df[util.EXTERNAL_SUPPLIER].fillna( util.NO )

        # Reorder columns
        df.insert( df.columns.get_loc( util.ACCOUNT_NUMBER ) + 1, util.EXTERNAL_SUPPLIER, df.pop( util.EXTERNAL_SUPPLIER ) )
        df.insert( df.columns.get_loc( util.EXTERNAL_SUPPLIER ) + 1, util.LOCATION_ADDRESS, df.pop( util.LOCATION_ADDRESS ) )

    # Names of columns representing monthly statistics start with 4-digit year.  Convert values from text to numeric.
    for col_name in df.columns:
        if col_name[:4].isnumeric() and ( df[col_name].dtype == object ):

            # Clean up text
            df[col_name] = df[col_name].replace( ',', '', regex=True )

            # Convert to numeric datatype
            df[col_name] = df[col_name].fillna( 0 ).astype( int )










    sr_b_duped = df.duplicated( subset=[util.ACCOUNT_NUMBER, 'cost_or_use'], keep=False )
    idx_duped = sr_b_duped[sr_b_duped].index.values
    df_dupes = df.iloc[ idx_duped ]
    # df_dupes = df_dupes.sort_values( by=[util.ACCOUNT_NUMBER, 'cost_or_use'] )
    # print( df_dupes )
    ls_drop_idx = []
    for idx, df_group in df_dupes.groupby( by=[util.ACCOUNT_NUMBER, 'cost_or_use'] ):
        print( '--------' )
        # print( df_group )
        print( idx )
        dc_zeroes = {}
        for index, row in df_group.iterrows():
            n_zeroes = 0
            for col_name in df_group.columns:
                if col_name[:4].isnumeric() and row[col_name] == 0:
                    n_zeroes += 1
            dc_zeroes[index] = n_zeroes
        print( dc_zeroes )
        for idx in dc_zeroes.keys():
            print( idx, dc_zeroes[idx] )
        index_with_most_zeroes = max( dc_zeroes, key=dc_zeroes.get )
        print( 'index with most zeroes:', index_with_most_zeroes )
        index_with_fewest_zeroes = min( dc_zeroes, key=dc_zeroes.get )
        print( 'index with fewest zeroes:', index_with_fewest_zeroes )
        del dc_zeroes[index_with_fewest_zeroes]
        print( dc_zeroes )
        ls_drop_idx.extend( list( dc_zeroes.keys() ) )
        print( 'keys to drop --> ', ls_drop_idx )

    print( '--------' )


    print( df.shape )
    print( df.index )
    df = df.drop( index=ls_drop_idx )
    print( df.shape )
    print( df.index )
    df = df.reset_index( drop=True )
    print( df.shape )
    print( df.index )
    





    # Save result to database
    util.create_table( args.output_table, conn, cur, df=df )

    # Report elapsed time
    util.report_elapsed_time()
