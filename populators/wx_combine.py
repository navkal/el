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
    parser = argparse.ArgumentParser( description='Combine weatherization tables' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve tables from database
    df_permits = pd.read_sql_table( 'BuildingPermits_L_Wx', engine, index_col=util.ID, parse_dates=True )
    df_projects = pd.read_sql_table( 'GlcacProjects_L', engine, index_col=util.ID, parse_dates=True )

    print( df_permits.shape )
    print( df_permits.columns )
    print( df_projects.shape )
    print( df_projects.columns )

    for how in ['left', 'right', 'inner', 'outer']:
        df_merge = pd.merge( df_permits, df_projects, how=how, on=[util.NORMALIZED_ADDRESS] )
        util.create_table( how.capitalize() + 'Merge', conn, cur, df=df_merge )
        print( df_merge.shape )
        print( df_merge.columns )

    util.report_elapsed_time()
