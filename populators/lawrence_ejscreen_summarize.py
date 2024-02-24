# Copyright 2024 Energize Andover.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append('../util')
import util

######################

# Main program
if __name__ == '__main__':

    # Retrieve arguments
    parser = argparse.ArgumentParser( description='Summarize Lawrence EJSCREEN data' )
    parser.add_argument( '-e', dest='ejscreen_filename',  help='Ejscreen database filename' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open Ejscreen database
    conn, cur, engine = util.open_database( args.ejscreen_filename, False )

    # Read full Ejscreen table from database
    df_ej = pd.read_sql_table( 'Ejscreen_L', engine, index_col=util.ID, parse_dates=True )

    # Get list of columns to drop from Ejscreen table
    df_drop = pd.read_sql_table( 'StatePercentilesDataset', engine, index_col=util.ID, parse_dates=True )
    ls_drop = list( df_drop[ df_drop['dropped'] == util.YES ][util.GDB_FIELDNAME] )

    # Drop columns from table
    df_ej = df_ej.drop( columns=ls_drop )

    # Save summary table to master database
    conn, cur, engine = util.open_database( args.master_filename, False )
    util.create_table( 'EjscreenSummary_L', conn, cur, df=df_ej )

    # Report elapsed time
    util.report_elapsed_time()
