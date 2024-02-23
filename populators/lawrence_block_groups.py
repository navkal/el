# Copyright 2024 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import geopandas as gpd

from shapely.geometry import Point

import os

import sys
sys.path.append('../util')
import util


LAWRENCE_MIN = 250100
LAWRENCE_MAX = 251899

LAT = util.LATITUDE
LONG = util.LONGITUDE
ZIP = util.ZIP
GEO = util.GEO_SERVICE

GEO_ID = util.CENSUS_GEO_ID
TRACT = util.CENSUS_TRACT
BLOCK_GROUP = util.CENSUS_BLOCK_GROUP

GEOID = 'GEOID'
TRACTCE = 'TRACTCE'
BLKGRPCE = 'BLKGRPCE'
GEOMETRY = 'geometry'

VSID = util.VISION_ID
ADDRESS = util.ADDRESS

def get_block_group( point ):

    bg = None

    for index, row in df_bg.iterrows():
        if row[GEOMETRY].contains( point ):
            bg = \
            {
                GEO_ID: row[GEOID],
                TRACT: row[TRACTCE],
                BLOCK_GROUP: row[BLKGRPCE]
            }
            break

    return bg


def get_block_groups_table():

    # Read raw block groups table from the shapefile
    df = gpd.read_file( args.block_groups_filename )

    # Extract rows pertaining to Lawrence
    df[TRACTCE] = df[TRACTCE].astype( int )
    df = df[ ( df[TRACTCE] >= LAWRENCE_MIN ) & ( df[TRACTCE] <= LAWRENCE_MAX ) ]
    df[TRACTCE] = ( df[TRACTCE] / 100 ).astype( int )

    return df


def get_parcels_table():

    # Get the parcels table
    conn, cur, engine = util.open_database( args.output_filename, False )
    df_parcels = pd.read_sql_table( 'GeoParcels_L', engine, index_col=util.ID )

    # Get geolocation data that has been encoded manually
    df_manual =  pd.read_excel( '../xl/lawrence/assessment/parcel_gelocation_overrides.xlsx' )
    df_manual[ZIP] = df_manual[ADDRESS].str.extract( r'(\d{5})$' )

    # Incorporate manual geolocations into parcels table
    for index, row in df_manual.iterrows():

        # Find target parcel row
        vsid = row[VSID]
        parcels_idx = df_parcels[df_parcels[VSID] == vsid].index

        # Copy values to target row
        df_parcels.loc[parcels_idx, LAT] = row[LAT]
        df_parcels.loc[parcels_idx, LONG] = row[LONG]
        df_parcels.loc[parcels_idx, ZIP] = row[ZIP]
        df_parcels.loc[parcels_idx, GEO] = 'Manual'

    # Ensure proper datatype for shapely operations
    df_parcels[LONG] = df_parcels[LONG].astype( 'float' )
    df_parcels[LAT] = df_parcels[LAT].astype( 'float' )

    # Initialize new columns
    df_parcels[TRACT] = None
    df_parcels[BLOCK_GROUP] = None

    return conn, cur, df_parcels


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Map parcel geolocations to US Census block groups' )
    parser.add_argument( '-b', dest='block_groups_filename',  help='Input filename - Name of shapefile containing Lawrence block group geometry', required=True )
    parser.add_argument( '-m', dest='output_filename',  help='Output filename - Name of master database file', required=True )
    args = parser.parse_args()

    df_bg = get_block_groups_table()
    conn, cur, df_parcels = get_parcels_table()

    n_found = 0
    n_failed = 0

    # Iterate over parcels that have geolocations
    df_geo = df_parcels.loc[ df_parcels[LAT].notnull() & df_parcels[LONG].notnull() ]

    print( 'Mapping {} geolocations to census block groups'.format( len( df_geo ) ) )

    for index, row in df_geo.iterrows():
        bg = get_block_group( Point( row[LONG], row[LAT] ) )
        if bg != None:
            n_found += 1
            df_parcels.at[index, GEO_ID] = bg[GEO_ID]
            df_parcels.at[index, TRACT] = bg[TRACT]
            df_parcels.at[index, BLOCK_GROUP] = bg[BLOCK_GROUP]
        else:
            n_failed += 1

        # print( '(+{},-{}) ({},{},{}): <{}>'.format( n_found, n_failed, df_parcels.loc[index][GEO_ID], df_parcels.loc[index][TRACT], df_parcels.loc[index][BLOCK_GROUP], df_parcels.loc[index][util.NORMALIZED_ADDRESS] ) )

    print( 'Mapped {} geolocations to census block groups'.format( n_found ) )

    # Save raw block groups table
    df_bg = util.prepare_for_database( df_bg, 'RawBlockGroups_L', exclude_unmapped=True )
    util.create_table( 'RawBlockGroups_L', conn, cur, df=df_bg )

    # Save parcels table
    df_parcels[GEO_ID] = df_parcels[GEO_ID].fillna(0).astype('int64')
    df_parcels[TRACT] = df_parcels[TRACT].fillna(0).astype(int)
    df_parcels[BLOCK_GROUP] = df_parcels[BLOCK_GROUP].fillna(0).astype(int)
    util.create_table( 'Parcels_L', conn, cur, df=df_parcels )

    util.report_elapsed_time()
