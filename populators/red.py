# Copyright 2020 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util



#------------------------------------------------------

###############
# Main script #
###############


if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Characterize partitions of residents by voting habits' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Read Residents table
    res_columns = \
    [
        util.RESIDENT_ID,
        util.PARTY_AFFILIATION,
        util.VOTED,
        util.VOTER_ENGAGEMENT_SCORE,
        util.PRIMARY_ELECTIONS_VOTED,
        util.LOCAL_ELECTIONS_VOTED,
        util.STATE_ELECTIONS_VOTED,
        util.SPECIAL_ELECTIONS_VOTED,
        util.TOWN_MEETINGS_ATTENDED,
        util.EARLY_VOTES,
        util.PRECINCT_NUMBER,
        util.AGE,
        util.GENDER,
        util.OCCUPATION,
        util.LAST_NAME,
        util.FIRST_NAME,
        util.NORMALIZED_STREET_NUMBER,
        util.RADDR_STREET_NUMBER_SUFFIX,
        util.LADDR_ALT_STREET_NUMBER,
        util.NORMALIZED_STREET_NAME,
        util.RADDR_APARTMENT_NUMBER,
        util.ZONING_CODE_1,
        util.LIKELY_DEM_SCORE,
        util.LEGACY_DEM_SCORE,
    ]
    df_red = pd.read_sql_table( 'Residents', engine, parse_dates=True, columns=res_columns )

    # Isolate likely red voters
    df_red = df_red[ ( df_red[util.LIKELY_DEM_SCORE] < 0 ) | ( df_red[util.LEGACY_DEM_SCORE] < 0 ) ]
    df_red[util.RED_SCORE] = df_red.apply( lambda row: -1 * ( row[util.LIKELY_DEM_SCORE] if ( row[util.LIKELY_DEM_SCORE] < 0 ) else row[util.LEGACY_DEM_SCORE] ), axis=1 )

    # Reorganize columns
    red_columns = \
    [
        util.RESIDENT_ID,
        util.PARTY_AFFILIATION,
        util.VOTED,
        util.RED_SCORE,
        util.VOTER_ENGAGEMENT_SCORE,
        util.PRIMARY_ELECTIONS_VOTED,
        util.LOCAL_ELECTIONS_VOTED,
        util.STATE_ELECTIONS_VOTED,
        util.SPECIAL_ELECTIONS_VOTED,
        util.TOWN_MEETINGS_ATTENDED,
        util.EARLY_VOTES,
        util.PRECINCT_NUMBER,
        util.AGE,
        util.GENDER,
        util.OCCUPATION,
        util.LAST_NAME,
        util.FIRST_NAME,
        util.NORMALIZED_STREET_NUMBER,
        util.RADDR_STREET_NUMBER_SUFFIX,
        util.LADDR_ALT_STREET_NUMBER,
        util.NORMALIZED_STREET_NAME,
        util.RADDR_APARTMENT_NUMBER,
        util.ZONING_CODE_1,
    ]
    df_red = df_red.reindex( columns=red_columns )


    # Save result to database
    util.create_table( 'Red', conn, cur, df=df_red )

    # Report elapsed time
    util.report_elapsed_time()
