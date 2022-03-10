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

    # Read table from database
    df_raw = pd.read_sql_table( 'RawLocalElectionResults', engine, index_col=util.ID, parse_dates=True )

    # Drop rows of totals
    df_raw = df_raw[ df_raw[util.OFFICE_OR_CANDIDATE] != 'Totals' ]

    # Create copy
    df = df_raw.copy()

    # Create empty columns
    df[util.OFFICE] = ''
    df[util.CANDIDATE] = ''
    df[util.TOTAL] = ''

    office = ''

    for index, row in df_raw.iterrows():
        if pd.isnull( row[util.PRECINCT_1] ):
            office = row[util.OFFICE_OR_CANDIDATE]
        else:
            df.at[index, util.OFFICE] = office
            df.at[index, util.CANDIDATE] = row[util.OFFICE_OR_CANDIDATE]
            df.at[index, util.TOTAL] = row[util.PRECINCT_1:util.PRECINCT_9].astype(int).sum()

    df = df.drop( columns=[util.OFFICE_OR_CANDIDATE] )
    df = df.dropna( subset=[util.PRECINCT_1] )

    # Save result to database
    util.create_table( 'LocalElectionResults', conn, cur, df=df )

    # Report elapsed time
    util.report_elapsed_time()
