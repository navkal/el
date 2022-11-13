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
    df = pd.merge( df_contracts, df_vendors, how='left', on=[util.VENDOR_NUMBER] )
    df = df.drop_duplicates( subset=[util.VENDOR_NUMBER] )

    # Fill in empty vendor names
    idx_empty = df[ pd.isnull( df[util.VENDOR_NAME] ) ].index
    df.at[idx_empty, util.VENDOR_NAME] = df.loc[idx_empty, util.VENDOR].str.extract( r'\([0-9]+\) (.*)', expand=False )

    # Initialize totals table
    df_totals = df.copy()
    df_totals = df_totals[ [util.VENDOR_NUMBER] ]

    # Group by vendor
    for idx_group_vn, df_group_vn in df.groupby( by=[util.VENDOR_NUMBER] ):

        # Get index into totals row
        vendor_number = df_group_vn[util.VENDOR_NUMBER].iloc[0]
        row_index = df_totals[ df_totals[util.VENDOR_NUMBER] == vendor_number ].index

        # Group by year
        for idx_group_yr, df_group_yr in df_group_vn.groupby( by=[util.YEAR] ):

            # Set total for current year
            year = df_group_yr[util.YEAR].iloc[0]
            column = str( year ) + '_' + util.AMOUNT
            year_total = df_group_yr[util.AMOUNT].sum()
            df_totals.at[row_index, column] = year_total

        # Set grand total for current vendor
        grand_total = df_group_vn[util.AMOUNT].sum()
        df_totals.at[row_index, util.AMOUNT] = grand_total

    # Sort totals columns
    df_totals = df_totals.reindex( sorted( df_totals.columns ), axis=1 )

    # Drop unwanted columns and merge totals
    df = df.drop( columns=[util.CONTRACT, util.METHOD, util.STATUS, util.DESCRIPTION, util.YEAR, util.PERIOD, util.AMOUNT, util.VENDOR, util.DEPARTMENT] )
    df = pd.merge( df, df_totals, how='left', on=[util.VENDOR_NUMBER] )

    # Save result to database
    util.create_table( args.output_table, conn, cur, df=df )

    # Report elapsed time
    util.report_elapsed_time()
