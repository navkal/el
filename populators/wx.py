# Copyright 2023 Energize Lawrence.  All rights reserved.

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

    # Read GLCAC weatherization jobs data
    print( '\n=======> GLCAC weatherization jobs input' )
    os.system( 'python xl_to_db.py -i ../xl/wx/glcac_jobs.xlsx -t RawGlcacJobs -o {0} -c'.format( args.master_filename ) )

    # Read weatherization permit data
    print( '\n=======> Weatherization permits input' )
    os.system( 'python xl_to_db.py -i ../xl/wx/building_permits_wx.xlsx -p "Work Description,Use of Property" -t RawBuildingPermits_Wx -o {0}'.format( args.master_filename ) )

    # Read past weatherization permit data
    print( '\n=======> Past weatherization permits input' )
    os.system( 'python xl_to_db.py -i ../xl/wx/building_permits_past_wx.xlsx -p "id" -t RawBuildingPermits_Past_Wx -o {0}'.format( args.master_filename ) )

    # Clean weatherization data
    print( '\n=======> Clean weatherization data' )
    os.system( 'python wx_clean.py -m {0}'.format( args.master_filename ) )

    # Combine weatherization data
    print( '\n=======> Combine weatherization data' )
    os.system( 'python wx_combine.py -m {0}'.format( args.master_filename ) )

    # Generate copyright notice
    print( '\n=======> Copyright' )
    df_about = pd.DataFrame( columns=['copyright'], data=['© 2023 Energize Lawrence.  All rights reserved.'] )
    util.create_about_table( 'Weatherization', df_about, args.master_filename )

    # Publish research copy of database
    input_db = util.read_database( args.master_filename )
    publish_info = \
    {
        'number_columns': True,
        'drop_table_names':
        [
            'RawBuildingPermits_Past_Wx',
            'RawBuildingPermits_Wx',
            'RawGlcacJobs',
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
