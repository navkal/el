# Copyright 2022 Energize Lawrence.  All rights reserved.

import argparse
import os
import pandas as pd

import sys
sys.path.append( '../util' )
import util


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Generate Lawrence master database' )
    parser.add_argument( '-m', dest='master_filename',  help='Output filename - Name of master database file', required=True )
    parser.add_argument( '-r', dest='research_filename',  help='Output filename - Name of research database file', required=True )
    args = parser.parse_args()

    # Read data sources
    print( '\n=======> Census' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/census.xlsx -t LawrenceCensus -o {0} -c'.format( args.master_filename ) )
    print( '\n=======> Housing input 1' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/assessment/housing_1.xlsx -t Housing_1 -r 1 -o {0}'.format( args.master_filename ) )
    print( '\n=======> Housing input 2' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/assessment/housing_2.xlsx -t Housing_2 -r 1 -o {0}'.format( args.master_filename ) )
    print( '\n=======> Housing input 3' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/assessment/housing_3.xlsx -t Housing_3 -r 1 -o {0}'.format( args.master_filename ) )

    # Generate table of housing assessments
    print( '\n=======> Housing merge' )
    os.system( 'python lawrence_housing.py -m {0}'.format( args.master_filename ) )

    # Generate copyright notice
    print( '\n=======> Copyright' )
    df_about = pd.DataFrame( columns=['copyright'], data=['© 2022 Energize Lawrence.  All rights reserved.'] )
    util.create_about_table( 'Lawrence', df_about, args.master_filename )

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
