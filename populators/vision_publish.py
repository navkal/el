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

    # Retrieve list of towns
    ls_towns = args.towns.split( ',' )

    # Initialize database create argument
    create = ' -c'

    ls_raw_table_names = []

    # Process list of tables
    print( '\n=======> Processing scraped tables from {}'.format( ls_towns ) )
    for town in ls_towns:
        town = town.capitalize()
        print( '\n=======> {}'.format( town ) )

        raw_table_name = 'Vision_Raw_{}'.format( town )
        ls_raw_table_names.append( raw_table_name )

        # Copy
        os.system( 'python db_to_db.py -i ../db/vision_{}.sqlite -f {} -t {} -o {} {}'.format( town.lower(), raw_table_name, raw_table_name, args.master_filename, create ) )

        # Clean
        os.system( 'python vision_clean.py -i {} -o Vision_{}  -l ../xl/residential_land_use_codes.xlsx -t {} -m {}'.format( raw_table_name, town, town, args.master_filename ) )

        # Clear database create argument
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
            * ls_raw_table_names
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
