# Copyright 2019 Energize Andover.  All rights reserved.

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
    parser = argparse.ArgumentParser( description='Generate summary of all elections recorded in database' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Read election tables from database
    election_columns = [util.ELECTION_DATE, util.ELECTION_TYPE]
    df_e1 = pd.read_sql_table( 'ElectionModel_01', engine, columns=election_columns )
    df_e2 = pd.read_sql_table( 'ElectionModel_02', engine, columns=election_columns )
    df_e3 = pd.read_sql_table( 'ElectionModel_03', engine, columns=election_columns )

    # Combine election data into one dataframe listing all elections, by date and type
    df_elections = df_e1.append( df_e2 ).append( df_e3 )
    df_history = df_elections.drop_duplicates( subset=[ util.ELECTION_DATE, util.ELECTION_TYPE ] )
    df_history = df_history.sort_values( by=[util.ELECTION_DATE] ).reset_index( drop=True )

    # Isolate election year
    df_history[util.ELECTION_YEAR] = df_history[util.ELECTION_DATE].str.split( '-', expand=True )[0].astype( int )

    # Count per-election turnout
    df_history[util.TURNOUT] = df_history.apply( lambda row: len( df_elections[ ( df_elections[util.ELECTION_DATE] == row[util.ELECTION_DATE] ) & ( df_elections[util.ELECTION_TYPE] == row[util.ELECTION_TYPE] ) ] ), axis=1 )

    # Generate factors for calculating voter engagement scores
    df_history[util.RECENCY_FACTOR] = df_history[util.ELECTION_YEAR] - df_history[util.ELECTION_YEAR].min() + 1
    df_history[util.TURNOUT_FACTOR] = round( 100 * df_history[util.TURNOUT].min() / df_history[util.TURNOUT] ).astype( int )
    df_history[util.SCORE] = df_history[util.TURNOUT_FACTOR] * df_history[util.RECENCY_FACTOR]

    # Save result to database
    util.create_table( 'ElectionHistory', conn, cur, df=df_history )

    # Report elapsed time
    util.report_elapsed_time()
