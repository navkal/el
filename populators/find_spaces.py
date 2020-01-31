# Copyright 2019 Energize Andover.  All rights reserved.

import argparse
import pandas as pd

import sys
sys.path.append( '../util' )
import util


#######################################


# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Find occurrences of multiple spaces in all tables of master database' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Fetch names of all tables
    cur.execute( 'SELECT name FROM sqlite_master WHERE type="table";' )
    rows = cur.fetchall()

    # Iterate through tables
    for row in rows:

        table_name = row[0]

        if table_name != 'sqlite_sequence':

            df = pd.read_sql_table( table_name, engine, index_col=util.ID, parse_dates=True )

            print( '\n--- {0} ---'.format( table_name ) )

            for col_name in df.columns:
                try:
                    sr = df[col_name].str.contains( '  ' )
                    b_has = True in sr.value_counts()
                except:
                    b_has = False

                if b_has:
                    print( col_name )


    # Report elapsed time
    util.report_elapsed_time()
