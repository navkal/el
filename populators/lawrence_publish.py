# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Generate Lawrence master database' )
    parser.add_argument( '-m', dest='master_filename',  help='Output filename - Name of master database file', required=True )
    parser.add_argument( '-r', dest='research_filename',  help='Output filename - Name of research database file', required=True )
    parser.add_argument( '-k', dest='leap_columns_keep',  help='List of LEAP columns to keep', required=True )
    parser.add_argument( '-l', dest='leap_filename',  help='Output filename - Name of LEAP database file', required=True )
    args = parser.parse_args()

    # Read all tables in master database
    input_db = util.read_database( args.master_filename )

    print( '\n=======> Publish research database' )

    # Publish research copy of database
    publish_info = \
    {
        'number_columns': True,
        'drop_table_names':
        [
            'Assessment_L_Commercial_Merged',
            'Assessment_L_Parcels_Merged',
            'Assessment_L_Residential_Merged',
            'GeoParcels_L',
            'RawBuildingPermits',
            'RawBuildingPermits_Cga',
            'RawBuildingPermits_Electrical',
            'RawBuildingPermits_Gas',
            'RawBuildingPermits_Plumbing',
            'RawBuildingPermits_Roof',
            'RawBuildingPermits_Siding',
            'RawBuildingPermits_Solar',
            'RawBuildingPermits_Sunrun',
            'RawBuildingPermits_Wx',
            'RawBuildingPermits_Wx_Ongoing',
            'RawBuildingPermits_Wx_Past',
            'RawBusinesses_1',
            'RawBusinesses_2',
            'RawCensus_L',
            'RawCommercial_1',
            'RawCommercial_2',
            'RawDpwVehicles_L',
            'RawEnergyMeterParticipation_L',
            'RawGlcacJobs',
            'RawNgAccountsBasic_L',
            'RawNgAccountsTps_L',
            'RawNgStreetNames_L',
            'RawOwnerOccupied_L',
            'RawResidential_1',
            'RawResidential_2',
            'RawResidential_3',
            'RawResidential_4',
            'RawResidential_5',
            'RawVehicleAttributes_L',
            'RawVehicleExciseTax_L',
            'RawWards_L',
            'VinDictionary_L',
         ],
        'encipher_column_names':
        [
        ],
        'drop_column_names':
        [
        ]
    }
    util.publish_database( input_db, args.research_filename, publish_info )


    print( '\n=======> Publish LEAP database' )

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve parcels table from database
    s_keep_table_name = 'Assessment_L_Parcels'
    df_parcels = pd.read_sql_table( s_keep_table_name, engine, index_col=util.ID, parse_dates=True )

    # Get list of columns to keep
    df_keep = pd.read_excel( args.leap_columns_keep, header=None, dtype=object )
    ls_keep = list( df_keep[0] )

    # Generate list of columns to drop
    ls_drop = [s for s in df_parcels.columns if s not in ls_keep]

    # Publish LEAP copy of database
    publish_info = \
    {
        'number_columns': False,
        'drop_table_names_complement': True,
        'drop_table_names':
        [
            s_keep_table_name,
            '_AboutLawrenceDatabase',
         ],
        'encipher_column_names':
        [
        ],
        'drop_column_names':
        [
            * ls_drop,
        ]
    }
    util.publish_database( input_db, args.leap_filename, publish_info )


    util.report_elapsed_time()
