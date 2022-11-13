# Copyright 2022 Energize Lawrence.  All rights reserved.

import argparse
import os
import pandas as pd

import sys
sys.path.append( '../util' )
import util


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Generate City Contracts master database' )
    parser.add_argument( '-m', dest='master_filename',  help='Output filename - Name of master database file', required=True )
    parser.add_argument( '-r', dest='research_filename',  help='Output filename - Name of research database file', required=True )
    args = parser.parse_args()

    # Read contracts data
    print( '\n=======> Contracts input' )
    os.system( 'python xl_to_db.py -i ../xl/city_contracts/contracts.xlsx -t RawContracts -m -o {0} -c'.format( args.master_filename ) )

    # Read vendors data
    print( '\n=======> Vendors input' )
    os.system( 'python xl_to_db.py -i ../xl/city_contracts/vendors.xlsx -t RawVendors -o {0}'.format( args.master_filename ) )

    # Clean Contracts table
    print( '\n=======> Clean Contracts table' )
    os.system( 'python city_contracts_clean_contracts.py -i RawContracts -o Contracts -d {0}'.format( args.master_filename ) )

    # Clean Vendors table
    print( '\n=======> Clean Vendors table' )
    os.system( 'python city_contracts_clean_vendors.py -i RawVendors -o Vendors -d {0}'.format( args.master_filename ) )

    # Generate City Contracts table
    print( '\n=======> Generate Cost History table' )
    os.system( 'python city_contracts_cost_history.py -c Contracts -v Vendors -o CostHistory -d {0}'.format( args.master_filename ) )


    # Publish research copy of database
    input_db = util.read_database( args.master_filename )
    publish_info = \
    {
        'number_columns': True,
        'drop_table_names':
        [
            'RawContracts',
            'RawVendors',
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
