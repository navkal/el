# Copyright 2023 Energize Andover.  All rights reserved.

import argparse
import pandas as pd

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


import sys
sys.path.append( '../util' )
import util

VSID = util.VISION_ID
ACCT = util.ACCOUNT_NUMBER
LOCN = util.LOCATION
OWN1 = util.OWNER_1_NAME
OWN2 = util.OWNER_2_NAME
OCCY = util.OCCUPANCY_HOUSEHOLDS
HEAT = util.HEATING_TYPE + util._DESC
FUEL = util.HEATING_FUEL + util._DESC
AIRC = util.AC_TYPE + util._DESC
HTAC = util.HEAT_AC
FLR1 = util.FIRST_FLOOR_USE
LAND = util.LAND_USE_CODE
BLDS = util.BUILDING_COUNT

#############

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Correlate account IDs in various tables of Lawrence assessment data' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    parser.add_argument( '-s', dest='scraped_filename',  help='Scraped database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Read tables of stitched-together assessment data
    df_res = pd.read_sql_table( 'Assessment_L_Residential', engine, index_col=util.ID )
    df_com = pd.read_sql_table( 'Assessment_L_Commercial', engine, index_col=util.ID )

    # Open the scraped database
    conn, cur, engine = util.open_database( args.scraped_filename, False )
    df_scr = pd.read_sql_table( 'LawrenceProperties', engine, index_col=util.ID )
    
    # Report initial conditions
    print( '' )
    print( 'res {}, com {}, scr {}'.format( len( df_res ), len( df_com ), len( df_scr ) ) )

    # Analyze content of dataframes
    print( 'Analysis based on {}:'.format( ACCT ) )

    df_rci = pd.merge( df_res, df_com, on=ACCT, how='inner' )
    df_rci = df_rci[ [ACCT] ]
    df_rci = pd.merge( df_rci, df_scr, on=ACCT, how='left' )
    df_rci = df_rci[ [ACCT, VSID, BLDS] ]
    print( 'Res/Com intersection: {}.  Building counts in range {}-{}.'.format( len( df_rci ), df_rci[BLDS].min(), df_rci[BLDS].max() ) )

    df_rcu = pd.merge( df_res, df_com, on=ACCT, how='outer' )
    print( 'Res/Com union: {}'.format( len( df_rcu ) ) )

    df_usi = pd.merge( df_rcu, df_scr, on=ACCT, how='inner' )
    print( 'Union/Scrape intersection: {}'.format( len( df_usi ) ) )

    df_usu = pd.merge( df_rcu, df_scr, on=ACCT, how='outer' )
    print( 'Union/Scrape union: {}'.format( len( df_usu ) ) )

    print( 'Number of new accounts: {}'.format( len( df_usu ) - len( df_rcu ) ) )

    # Report elapsed time
    util.report_elapsed_time()
