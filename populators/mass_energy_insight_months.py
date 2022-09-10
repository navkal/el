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
    parser.add_argument( '-d', dest='db_filename', help='Database filename' )
    parser.add_argument( '-i', dest='input_table', help='Input table name' )
    parser.add_argument( '-o', dest='output_table', help='Output table name' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read table from database
    df_org = pd.read_sql_table( args.input_table, engine, index_col=util.ID )

    # Initialize base dataframe, omitting unwanted columns
    df_base = df_org.drop( columns=[util.ONE, util.USAGE_END_NULL] )

    # Build dictionary of months covered by table data and drop all month columns from base dataframe
    months = {}
    for col_name in df_org.columns:
        suffix = col_name[-2:]

        if suffix.isnumeric():
            if suffix not in months:
                months[suffix] = []
            months[suffix].append( col_name )

            df_base = df_base.drop( columns=[col_name] )

    # Build per-month dataframes and save to database
    for month in months:

        # Create copy of base dataframe
        df_month = df_base.copy()

        # Copy columns pertaining to current month in dataframe
        df_month[months[month]] = df_org[months[month]].copy()

        # Save result to database
        util.create_table( args.output_table + '_' + month, conn, cur, df=df_month )


    # Report elapsed time
    util.report_elapsed_time()
