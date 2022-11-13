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
    parser = argparse.ArgumentParser( description='Clean up raw vendors table' )
    parser.add_argument( '-d', dest='db_filename', help='Database filename' )
    parser.add_argument( '-c', dest='input_contracts_table', help='Input table name: contracts' )
    parser.add_argument( '-v', dest='input_vendors_table', help='Input table name: vendors' )
    parser.add_argument( '-o', dest='output_table', help='Output table name' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read tables from database
    df_contracts = pd.read_sql_table( args.input_contracts_table, engine, index_col=util.ID )
    df_vendors = pd.read_sql_table( args.input_vendors_table, engine, index_col=util.ID )

    # Merge vendors to contracts
    df_merge = pd.merge( df_contracts, df_vendors, how='left', on=[util.VENDOR_NUMBER] )

    # Group by vendor
    for idx_group_vn, df_group_vn in df_merge.groupby( by=[util.VENDOR_NUMBER] ):
        print( '=================' )
        # Group by year
        for idx_group_yr, df_group_yr in df_group_vn.groupby( by=[util.YEAR] ):
            print( df_group_yr[ [util.VENDOR_NUMBER, util.YEAR, util.AMOUNT] ] )
            print( 'Vendor: {}, Year: {}, Total: ${}'.format( df_group_vn[util.VENDOR_NUMBER].iloc[0],  df_group_yr[util.YEAR].iloc[0], df_group_yr[util.AMOUNT].sum() ) )
            print( '------------' )
        print( '------------' )
        print( 'Vendor Grand Total: ${}'.format( df_group_vn[util.AMOUNT].sum() ) )
        print( '=================' )

    exit()


    # Save result to database
    util.create_table( args.output_table, conn, cur, df=df )

    # Report elapsed time
    util.report_elapsed_time()
