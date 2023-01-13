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

    # Read census data
    print( '\n=======> Census input' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/census.xlsx -n "Res. ID" -s "Res. ID" -t RawCensus -o {0} -c'.format( args.master_filename ) )

    # Generate Census table
    print( '\n=======> Census table' )
    os.system( 'python lawrence_census.py -m {0}'.format( args.master_filename ) )

    # Read parcels data
    print( '\n=======> Parcels tables' )
    os.system( 'python lawrence_parcels_finish.py -d ../db/lawrence_parcels.sqlite'.format( args.master_filename ) )
    os.system( 'python db_to_db.py -i ../db/lawrence_parcels.sqlite -f "Parcels_L" -t "Assessment_L_Parcels" -o {0}'.format( args.master_filename ) )
    os.system( 'python db_to_db.py -i ../db/lawrence_parcels.sqlite -f "ParcelSummary" -t "ParcelSummary" -o {0}'.format( args.master_filename ) )

    # Read residential assessment data
    print( '\n=======> Residential input 1' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/assessment/residential_1.xlsx -t RawResidential_1 -r 1 -o {0}'.format( args.master_filename ) )
    print( '\n=======> Residential input 2' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/assessment/residential_2.xlsx -t RawResidential_2 -r 1 -o {0}'.format( args.master_filename ) )
    print( '\n=======> Residential input 3' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/assessment/residential_3.xlsx -t RawResidential_3 -r 1 -o {0}'.format( args.master_filename ) )
    print( '\n=======> Residential input 4' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/assessment/residential_4_5.txt -t RawResidential_4 -v -f "|" -x -o {0}'.format( args.master_filename ) )
    print( '\n=======> Residential input 5' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/assessment/residential_4_5.txt -t RawResidential_5 -v -f "|" -x -o {0}'.format( args.master_filename ) )

    # Generate table of residential assessments
    print( '\n=======> Residential merge' )
    os.system( 'python lawrence_residential.py -m {0}'.format( args.master_filename ) )

    # Read commercial assessment data
    print( '\n=======> Commercial input 1' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/assessment/commercial_1.xlsx -t RawCommercial_1 -r 2 -n Location -o {0}'.format( args.master_filename ) )
    print( '\n=======> Commercial input 2' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/assessment/commercial_2.xlsx -t RawCommercial_2 -r 2 -k "REM_ACCT_NUM,REM_USE_CODE,CNS_OCC,CNS_OCC_DESC" -n REM_USE_CODE -o {0}'.format( args.master_filename ) )

    # Generate table of commercial assessments
    print( '\n=======> Commercial merge' )
    os.system( 'python lawrence_commercial.py -m {0}'.format( args.master_filename ) )

    # Correct mis-classification of residential and commercial assessment records
    os.system( 'python lawrence_land_use.py -l ../xl/lawrence/assessment/residential_land_use_codes.xlsx -m {0}'.format( args.master_filename ) )

    # Read business registration data
    print( '\n=======> Businesses input 1' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/businesses/businesses_1.xlsx -s License# -t RawBusinesses_1 -o {0}'.format( args.master_filename ) )
    print( '\n=======> Businesses input 2' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/businesses/businesses_2.xlsx -s License# -t RawBusinesses_2 -o {0}'.format( args.master_filename ) )

    # Generate expanded Businesses table
    print( '\n=======> Businesses merge' )
    os.system( 'python lawrence_businesses.py -m {0}'.format( args.master_filename ) )

    # Read city building permit data
    print( '\n=======> City Building Permit input' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/building_permits/building_permits.xlsx -n "Permit#" -s "Permit#" -t RawBuildingPermits -o {0}'.format( args.master_filename ) )

    # Generate city Building Permits table
    print( '\n=======> City Building Permits table' )
    os.system( 'python lawrence_building_permits.py -m {0}'.format( args.master_filename ) )

    # Read Columbia Gas building permit data
    print( '\n=======> Columbia Gas Building Permit input' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/building_permits/building_permits_columbia_gas.xls -n "Permit #" -u "City/Town,Address Num,Street" -s "Date,Permit #,Address Num,Street" -t RawBuildingPermits_Cga -o {0}'.format( args.master_filename ) )

    # Generate Columbia Gas Building Permits table
    print( '\n=======> Columbia Gas Building Permits table' )
    os.system( 'python lawrence_building_permits_cga.py -m {0}'.format( args.master_filename ) )

    # Read solar building permit data
    print( '\n=======> Solar Building Permit input' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/building_permits/building_permits_solar.xlsx -p "Permit Type,Subtype,Use of Property" -t RawBuildingPermits_Solar -o {0}'.format( args.master_filename ) )

    # Generate Solar Building Permits table
    print( '\n=======> Solar Building Permits table' )
    os.system( 'python lawrence_building_permits_solar.py -m {0}'.format( args.master_filename ) )

    # Read Sunrun data
    print( '\n=======> Sunrun Building Permits input' )
    os.system( 'python xl_to_db.py -i ../xl/lawrence/building_permits/building_permits_sunrun.xlsx -y -t RawBuildingPermits_Sunrun -o {0}'.format( args.master_filename ) )

    # Generate Sunrun Building Permits table
    print( '\n=======> Sunrun Building Permits table' )
    os.system( 'python lawrence_building_permits_sunrun.py -m {0}'.format( args.master_filename ) )

    # Generate copyright notice
    print( '\n=======> Copyright' )
    df_about = pd.DataFrame( columns=['copyright'], data=['© 2023 Energize Lawrence.  All rights reserved.'] )
    util.create_about_table( 'Lawrence', df_about, args.master_filename )

    # Publish research copy of database
    input_db = util.read_database( args.master_filename )
    publish_info = \
    {
        'number_columns': True,
        'drop_table_names':
        [
            'Assessment_L_Commercial_Merged',
            'Assessment_L_Residential_Merged',
            'RawBuildingPermits',
            'RawBuildingPermits_Cga',
            'RawBusinesses_1',
            'RawBusinesses_2',
            'RawCensus',
            'RawCommercial_1',
            'RawCommercial_2',
            'RawResidential_1',
            'RawResidential_2',
            'RawResidential_3',
            'RawResidential_4',
            'RawResidential_5',
            'RawBuildingPermits_Solar',
            'RawBuildingPermits_Sunrun',
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
