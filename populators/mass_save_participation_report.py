# Copyright 2025 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import numpy as np

import sys
sys.path.append( '../util' )
import util


LS_TABLES = \
[
    'ParticipationCommercialElectric',
    'ParticipationCommercialGas',
    'ParticipationResidentialElectric',
    'ParticipationResidentialGas',
]

#------------------------------------------------------

if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Process Mass Save Participation Report' )
    parser.add_argument( '-d', dest='db_filename',  help='Database filename' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    for s_table in LS_TABLES:

        # Read the raw table
        df = pd.read_sql_table( f'Raw{s_table}', engine, index_col=util.ID )

        # Clear out asterisks and empty strings
        df = df.replace( '*' , '' )
        df = df.replace( '' , np.nan )

        # Drop rows that don't have interesting data
        ls_ignore_cols = \
        [
            util.TOWN_NAME,
            util.ELECTRIC_UTILITY if util.ELECTRIC_UTILITY in df.columns else util.GAS_UTILITY,
            util.YEAR,
        ]
        ls_dropna_cols = df.columns.difference( ls_ignore_cols )
        df = df.dropna( subset=ls_dropna_cols, how='all' )

        # Drop rows that represent totals
        df_total = df[df[util.TOWN_NAME] == 'Total']
        df = df.drop( index=df_total.index )

        # Drop empty columns
        df = df.dropna( axis='columns', how='all' )

        # Sort based on column order
        df = df.sort_values( by=util.COLUMN_ORDER[s_table] )

        # Fix numeric columns
        df = util.fix_numeric_columns( df )

        # Save the cleaned table
        util.create_table( s_table, conn, cur, df=df )

    # Report elapsed time
    util.report_elapsed_time()
