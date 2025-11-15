# Copyright 2025 Energize Andover.  All rights reserved.

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
    parser = argparse.ArgumentParser( description='Summarize MassSave analysis' )
    parser.add_argument( '-d', dest='db_filename',  help='Database filename' )
    parser.add_argument( '-s', dest='sector',  help='Sector to summarize' )
    parser.add_argument( '-t', dest='table',  help='Summary table' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read Analysis table from database
    df_analysis = pd.read_sql_table( 'Analysis', engine, index_col=util.ID )

    # Create report table
    df_report = df_analysis[ [util.YEAR, util.COUNTY, util.TOWN_NAME, util.SECTOR, util.ANNUAL_ELECTRIC_USAGE, util.ANNUAL_ELECTRIC_SAVINGS, util.ELECTRIC_INCENTIVES] ]
    df_report = df_report[df_report[util.SECTOR] == args.sector]
    df_report = df_report.drop( columns=[util.SECTOR] )

    # Save report to database
    util.create_table( args.table, conn, cur, df=df_report )

    # Report elapsed time
    util.report_elapsed_time()
