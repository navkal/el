# Copyright 2022 Energize Andover.  All rights reserved.

import argparse
import os

import sys
sys.path.append( '../util' )
import util


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Generate Lawrence master database' )
    parser.add_argument( '-m', dest='master_filename',  help='Output filename - Name of master database file', required=True )
    parser.add_argument( '-r', dest='research_filename',  help='Output filename - Name of research database file', required=True )
    args = parser.parse_args()

    # Generate Residences table
    print( '\n=======> Residences' )
    os.system( 'python lawrence_residences.py -m {0}'.format( args.master_filename ) )

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
