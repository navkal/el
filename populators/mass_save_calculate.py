# Copyright 2022 Energize Andover.  All rights reserved.

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
    parser = argparse.ArgumentParser( description='Make calculations based on MassSave data' )
    parser.add_argument( '-d', dest='db_filename',  help='Database filename' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read tables from database
    df_gr = pd.read_sql_table( 'GeographicReport', engine, index_col=util.ID )
    df_towns = pd.read_sql_table( 'Towns', engine, index_col=util.ID )
    df_ees = pd.read_sql_table( 'EnergyEfficiencySurcharge', engine, index_col=util.ID )

    print( df_gr )
    print( df_towns )
    print( df_ees )

    exit()

    for index, row in df_towns.iterrows():
        town = row[util.TOWN_NAME]
        pct_eb = row[util.PCT_ENERGY_BURDENED]
        if town == 'Lawrence':
            print( '!!!' )
            print( town )
            print( pct_eb )
            exit()












    exit()







    util.create_table( "Towns", conn, cur, df=df_towns )

    # Report elapsed time
    util.report_elapsed_time()
