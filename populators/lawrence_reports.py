# Copyright 2025 Energize Andover.  All rights reserved.

import argparse
import os

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sqlite3
import sqlalchemy

import sys
sys.path.append( '../util' )
import util


# Prototype: Generate a test report
def make_test_report( conn, cur, engine, output_directory ):

    df_test_report = pd.read_sql_table( 'BuildingPermits_L_Wx', engine, index_col=util.ID, parse_dates=True )

    import random
    df_test_report = df_test_report.iloc[:random.randint(5,20), :random.randint(5,7) ]

    # Save report to database
    util.create_table( 'TEST_REPORT', conn, cur, df=df_test_report )

    # Save report to excel file
    filename = 'TEST_REPORT.xlsx'
    filepath = os.path.join( output_directory, filename )

    df_test_report.to_excel( filepath, index=False )


######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate reports based on contents of master database' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename', required=True )
    parser.add_argument( '-o', dest='output_directory', help='Target directory output files', required=True )
    parser.add_argument( '-c', dest='clear_directory', action='store_true', help='Clear target directory first?' )

    args = parser.parse_args()

    # Optionally clear target directory
    if args.clear_directory:
        util.clear_directory( args.output_directory )

    # Report arguments
    print( '' )
    print( 'Arguments' )
    print( ' Input database:', args.master_filename )
    print( ' Output directory:', args.output_directory )


    # Open the database
    conn, cur, engine = util.open_database( args.master_filename, False )

    make_test_report( conn, cur, engine, args.output_directory )

    util.report_elapsed_time()
