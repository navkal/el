# Copyright 2025 Energize Andover.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util


##################################

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Finish generating Lawrence Assessment Parcels table' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve parcels table from database
    df_parcels = pd.read_sql_table( 'Assessment_L_Parcels_Merged', engine, index_col=util.ID, parse_dates=True )

    # Retrieve Health Risk Score from EJ Screen Summary table
    df_score = pd.read_sql_table( 'EJScreenSummary_L', engine, columns=[util.CENSUS_GEO_ID, util.HEALTH_RISK_SCORE] )

    # Merge health risk score to parcels table
    df_parcels = pd.merge( df_parcels, df_score, how='left', on=[util.CENSUS_GEO_ID] )
    df_parcels[util.HEALTH_RISK_SCORE] = df_parcels[util.HEALTH_RISK_SCORE].fillna( 0 ).astype( int )

    # Save final table of commercial assessments
    util.create_table( 'Assessment_L_Parcels', conn, cur, df=df_parcels )

    util.report_elapsed_time()
