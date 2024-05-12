# Copyright 2024 Energize Andover.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append('../util')
import util


DPW_COLUMNS = \
[
    util.DEPARTMENT,
    util.MODEL,
    util.REG_ID_PLATE,
]

######################

# Main program
if __name__ == '__main__':

    # Retrieve arguments
    parser = argparse.ArgumentParser( description='Combine city vehicle data' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Read input tables from database
    df_tax = pd.read_sql_table( 'RawVehicleExciseTax_L', engine, index_col=util.ID, parse_dates=True )
    df_dpw = pd.read_sql_table( 'RawDpwVehicles_L', engine, index_col=util.ID, parse_dates=True, columns=DPW_COLUMNS )
    df_vin = pd.read_sql_table( 'VinDictionary_L', engine, index_col=util.ID, parse_dates=True )

    # Merge
    df = pd.merge( df_tax, df_dpw, how='left', on=[util.REG_ID_PLATE] )
    df = pd.merge( df, df_vin, how='left', on=[util.REG_ID_PLATE] )

    # Sort
    df = df.sort_values( by=list( df.columns ) )

    # Save to database
    util.create_table( 'CityVehicles_L', conn, cur, df=df )

    # Report elapsed time
    util.report_elapsed_time()
