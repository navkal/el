# Copyright 2019 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
import re

import sys
sys.path.append( '../util' )
import util


#######################################

# Global data structure

PUBLISH_INFO = \
{
    'student': \
    {
        'drop_table_names':
        [
            'Employees'
        ],
        'encipher_column_names':
        [
            util.RESIDENT_ID
        ],
        'drop_column_names':
        [
            util.FIRST_NAME,
            util.MIDDLE_NAME,
            util.LAST_NAME,
            util.DATE_OF_BIRTH,
            util.OWNER_1_NAME,
            util.OWNER_2_NAME,
            util.OWNER_3_NAME,
            util.PHONE,
            util.GRANTOR,
            util.PREVIOUS_GRANTOR,
            util.PARTY_AFFILIATION,
            util.PARTY_PREFERENCE_SCORE,
            util.LEGACY_PREFERENCE_SCORE,
            util.GENDER,
            util.IS_FAMILY,
            util.LIKELY_DEM_SCORE,
            util.LEGACY_DEM_SCORE,
            util.D,
            util.R,
            util.ABSENT,
            util.VOTED_BOTH,
            util.CHANGED_AFFILIATION,
            util.PARTY_VOTED_HISTORY,
            util.LIKELY_DEM_COUNT,
            util.LIKELY_REPUB_COUNT,
            util.LOCAL_DEM_VOTER_COUNT,
            util.LOCAL_REPUB_VOTER_COUNT,
            util.MEAN_LIKELY_DEM_SCORE,
            util.MEAN_PARTY_PREFERENCE_SCORE,
            util.MEAN_LIKELY_DEM_VOTER_ENGAGEMENT_SCORE,
            util.MEAN_LIKELY_REPUB_VOTER_ENGAGEMENT_SCORE,
        ]
    },
    'town': \
    {
        'drop_table_names':
        [
            'Employees',
            'Gender_2014',
            'Lookup'
        ],
        'encipher_column_names':
        [
        ],
        'drop_column_names':
        [
            util.PARTY_AFFILIATION,
            util.PARTY_PREFERENCE_SCORE,
            util.LEGACY_PREFERENCE_SCORE,
            util.GENDER,
            util.IS_FAMILY,
            util.LIKELY_DEM_SCORE,
            util.LEGACY_DEM_SCORE,
            util.D,
            util.R,
            util.ABSENT,
            util.VOTED_BOTH,
            util.CHANGED_AFFILIATION,
            util.PARTY_VOTED_HISTORY,
            util.LIKELY_DEM_COUNT,
            util.LIKELY_REPUB_COUNT,
            util.LOCAL_DEM_VOTER_COUNT,
            util.LOCAL_REPUB_VOTER_COUNT,
            util.MEAN_LIKELY_DEM_SCORE,
            util.MEAN_PARTY_PREFERENCE_SCORE,
            util.MEAN_LIKELY_DEM_VOTER_ENGAGEMENT_SCORE,
            util.MEAN_LIKELY_REPUB_VOTER_ENGAGEMENT_SCORE,
        ]
    }
}






#######################################


def publish_database( input_db, output_filename, publish_info ):

    drop_table_names = publish_info['drop_table_names']
    drop_column_names = publish_info['drop_column_names']
    encipher_column_names = publish_info['encipher_column_names']

    print( '' )
    print( 'Publishing database', output_filename )
    print( '- Dropping tables {0}'.format( drop_table_names ) )
    print( '- Dropping columns {0}'.format( drop_column_names ) )
    print( '- Enciphering columns {0}'.format( encipher_column_names ) )
    print( '' )

    output_db = {}
    for table_name in input_db:
        if table_name not in drop_table_names:
            print( 'Sanitizing table {0}'.format( table_name ) )
            output_db[table_name] = input_db[table_name].drop( columns=drop_column_names, errors='ignore' )
            for col_name in encipher_column_names:
                output_db[table_name] = util.encipher_column( output_db[table_name], col_name )


    # Open output database
    conn, cur, engine = util.open_database( output_filename, True )

    # Save result to database
    print( '' )
    for table_name in output_db:
        print( 'Publishing table', table_name )
        df = output_db[table_name]
        df.to_sql( table_name, conn, index=False )


#######################################


# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Generate copy of master database for publication' )
    parser.add_argument( '-i', dest='input_filename',  help='Input filename' )
    parser.add_argument( '-o', dest='output_folder',  help='Output folder' )
    parser.add_argument( '-d', dest='debug', action='store_true', help='Generate debug versions of databases?' )
    args = parser.parse_args()

    # Read input database
    input_db = util.read_database( args.input_filename )

    # Publish output databases
    debug = '_debug' if args.debug else ''
    for output_db_name, publish_info in PUBLISH_INFO.items():
        publish_database( input_db, args.output_folder + '/' + output_db_name + debug + '.sqlite', publish_info )

    # Report elapsed time
    util.report_elapsed_time()

