# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Extend table of weatherization permits' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve weatherization permit table
    df_permits = pd.read_sql_table( 'BuildingPermits_L_Wx', engine, index_col=util.ID, parse_dates=True )

    # Retrieve pertinent columns of assessment table
    df_parcels = pd.read_sql_table( 'Assessment_L_Parcels', engine, index_col=util.ID, columns=[util.ACCOUNT_NUMBER, * util.WX_EXTENDED], parse_dates=True )

    # Merge assessment table fields into weatherization permit table
    df_permits = pd.merge( df_permits, df_parcels, how='left', on=[util.ACCOUNT_NUMBER] )

    # Create table in database
    util.create_table( 'BuildingPermits_L_Wx_Extended', conn, cur, df=df_permits )

    util.report_elapsed_time()
