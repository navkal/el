# Copyright 2022 Energize Andover.  All rights reserved.

import argparse

import sys
sys.path.append( '../util' )
import util



# Information on how to publish databases
PUBLISH_INFO = \
{
    'mass_save_ees_analysis': \
    {
        'number_columns': True,
        'drop_table_names':
        [
            'RawElectricUsage',
            'RawGasUsage',
            'ElectricUsage',
            'GasUsage',
        ],
        'encipher_column_names':
        [
        ],
        'drop_column_names':
        [
        ]
    },
}


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
        util.publish_database( input_db, args.output_folder + '/' + output_db_name + debug + '.sqlite', publish_info )

    # Report elapsed time
    util.report_elapsed_time()

