# Copyright 2020 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util



#------------------------------------------------------

def count_recent_local_votes( row ):

    count = 0

    for col in recent_local_columns:
        if row[col] == 'Y':
            count += 1

    return count


#------------------------------------------------------

###############
# Main script #
###############


if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Generate partisan tables' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    parser.add_argument( '-p', dest='party',  help='D or R', required=True )
    args = parser.parse_args()

    if args.party == util.D:
        dem = True
    elif args.party == util.R:
        dem = False
    else:
        print( 'Unrecognized party:', args.party )
        exit()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Read Residents table
    df = pd.read_sql_table( 'Residents', engine, parse_dates=True )

    # Isolate likely partisans
    if dem:
        df = df[ ( df[util.LIKELY_DEM_SCORE] > 0 ) | ( df[util.LEGACY_DEM_SCORE] > 0 ) ]
        df[util.PARTISAN_SCORE] = df.apply( lambda row: ( row[util.LIKELY_DEM_SCORE] if ( row[util.LIKELY_DEM_SCORE] > 0 ) else row[util.LEGACY_DEM_SCORE] ), axis=1 )
    else:
        df = df[ ( df[util.LIKELY_DEM_SCORE] < 0 ) | ( df[util.LEGACY_DEM_SCORE] < 0 ) ]
        df[util.PARTISAN_SCORE] = df.apply( lambda row: -1 * ( row[util.LIKELY_DEM_SCORE] if ( row[util.LIKELY_DEM_SCORE] < 0 ) else row[util.LEGACY_DEM_SCORE] ), axis=1 )

    # Count votes in recent local elections
    recent_local_columns = []
    for col in df.columns:
        if col.startswith( util.VOTED + '_local_' ):
            recent_local_columns.append( col )
    recent_local_columns = recent_local_columns[ ( -1 * util.RECENT_LOCAL_ELECTION_COUNT ) : ]
    df[util.RECENT_LOCAL_ELECTIONS_VOTED] = df.apply( lambda row: count_recent_local_votes( row ), axis=1 )

    # Find columns counting attendance at recent town meetings
    recent_meeting_columns = []
    for col in df.columns:
        if col.startswith( util.TOWN_MEETINGS_ATTENDED + '_' ):
            recent_meeting_columns.append( col )

    # Set columns for partisan table
    columns_bf = \
    [
        util.RESIDENT_ID,
        util.PARTISAN_SCORE,
        util.VOTER_ENGAGEMENT_SCORE,
        util.RECENT_LOCAL_ELECTIONS_VOTED,
        util.PARTY_AFFILIATION,
        util.VOTED,
        util.PRIMARY_ELECTIONS_VOTED,
        util.LOCAL_ELECTIONS_VOTED,
        util.STATE_ELECTIONS_VOTED,
        util.SPECIAL_ELECTIONS_VOTED,
        util.TOWN_MEETINGS_ATTENDED,
    ]

    columns_af = \
    [
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
        util.ZONING_CODE_1
    ]

    df = df.reindex( columns=( columns_bf + recent_local_columns + recent_meeting_columns + columns_af ) )

    # Sort on partisan score
    df = df.sort_values( by=[util.PARTISAN_SCORE, util.VOTER_ENGAGEMENT_SCORE, util.RECENT_LOCAL_ELECTIONS_VOTED], ascending=False )

    # Save result to database
    util.create_table( 'Partisans_' + args.party, conn, cur, df=df )

    # Report elapsed time
    util.report_elapsed_time()
