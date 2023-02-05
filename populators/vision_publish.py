# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse
import os
import pandas as pd

import sys
sys.path.append( '../util' )
import util

# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Generate Vision master and research databases' )
    parser.add_argument( '-m', dest='master_filename',  help='Output filename - Name of master database file', required=True )
    parser.add_argument( '-r', dest='research_filename',  help='Output filename - Name of research database file', required=True )
    parser.add_argument( '-t', dest='towns',  help='List of towns to include', required=True )
    args = parser.parse_args()

    create = ' -c'
    ls_towns = args.towns.split( ',' )

    # Copy tables
    print( '\n=======> Copying tables' )
    for town in ls_towns:
        town = town.capitalize()
        print( '\n-------> {}'.format( town ) )
        os.system( 'python db_to_db.py -i ../db/vision_{}.sqlite -f Vision_{} -t Vision_{} -o {} {}'.format( town.lower(), town, town, args.master_filename, create ) )
        create = ''

    # Generate copyright notice
    print( '\n=======> Copyright' )
    util.create_about_table( 'Vision', util.make_df_about_energize_lawrence(), args.master_filename )

    # Publish research copy of database
    input_db = util.read_database( args.master_filename )
    publish_info = \
    {
        'number_columns': True,
        'drop_table_names':
        [
        ],
        'encipher_column_names':
        [
        ],
        'drop_column_names':
        [
        ]
    }
    util.publish_database( input_db, args.research_filename, publish_info )

    util.report_elapsed_time()
