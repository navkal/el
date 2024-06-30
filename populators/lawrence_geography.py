# Copyright 2024 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import geopandas as gpd

from shapely.geometry import Point, Polygon
from pyproj import Transformer

import os

import sys
sys.path.append('../util')
import util

WARD = util.WARD_NUMBER
PRECINCT = util.PRECINCT_NUMBER

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


def get_block_groups_table():

    # Read raw block groups table from the shapefile
    df = gpd.read_file( args.block_groups_filename )

    # Extract rows pertaining to Lawrence
    df[TRACTCE] = df[TRACTCE].astype( int )
    df = df[ ( df[TRACTCE] >= LAWRENCE_MIN ) & ( df[TRACTCE] <= LAWRENCE_MAX ) ]
    df[TRACTCE] = ( df[TRACTCE] / 100 ).astype( int )

    return df


def map_point_to_block_group( point, df_bg ):

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


def get_wards_table():

    # Read raw wards table from the shapefile
    df = gpd.read_file( args.wards_filename )

    # Extract rows pertaining to Lawrence
    df = df[ df['TOWN'] == 'LAWRENCE' ]

    # Transform from projection coordinates to latitude/longitude
    transformer = Transformer.from_crs( 'epsg:26986', 'epsg:4326', always_xy=True )

    for index, row in df.iterrows():

        # Reorganize current polygon values into list of tuples
        shape = row[GEOMETRY]
        xx, yy = shape.exterior.coords.xy
        ls_x = xx.tolist()
        ls_y = yy.tolist()
        ls_xy = [ ( ls_x[i], ls_y[i] ) for i in range( 0, len( ls_x ) ) ]

        # Transform coordinates
        lat_long = [ transformer.transform( x, y ) for x, y in ls_xy ]

        # Save transformed coordinates in dataframe
        df.at[index, GEOMETRY] = Polygon( lat_long )

    # Return dataframe
    return df


def map_point_to_ward( point, df_wp ):

    wp = None

    for index, row in df_wp.iterrows():
        if row[GEOMETRY].contains( point ):
            wp = \
            {
                WARD: row['WARD'],
                PRECINCT: row['PRECINCT']
            }
            break

    return wp


def get_parcels_table():

    # Get the parcels table
    conn, cur, engine = util.open_database( args.output_filename, False )
    df_parcels = pd.read_sql_table( 'GeoParcels_L', engine, index_col=util.ID )

    # Get geolocation data that has been encoded manually
    df_manual =  pd.read_excel( '../xl/lawrence/assessment/parcel_geolocation_manual_overrides.xlsx' )
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

    # Ensure proper datatype and precision for shapely operations
    df_parcels[LONG] = df_parcels[LONG].astype(float).round( decimals=5 )
    df_parcels[LAT] = df_parcels[LAT].astype(float).round( decimals=5 )

    return conn, cur, df_parcels


def map_parcels_to_regions( df_parcels, df_geo, df_regions, map_point_to_region, ls_columns ):

    # Initialize new columns
    for col in ls_columns:
        df_parcels[col] = None

    n_found = 0
    n_failed = 0

    # Iterate over parcels that have geolocations
    for index, row in df_geo.iterrows():
        region = map_point_to_region( Point( row[LONG], row[LAT] ), df_regions )
        if region != None:
            n_found += 1
            for col in ls_columns:
                df_parcels.at[index, col] = region[col]
        else:
            n_failed += 1

    print( 'Found {} mappings'.format( n_found ) )

    return df_parcels


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Map parcel geolocations to US Census block groups' )
    parser.add_argument( '-b', dest='block_groups_filename',  help='Input filename - Name of shapefile containing Lawrence block group geometry', required=True )
    parser.add_argument( '-w', dest='wards_filename',  help='Input filename - Name of shapefile containing Lawrence ward geometry', required=True )
    parser.add_argument( '-m', dest='output_filename',  help='Output filename - Name of master database file', required=True )
    args = parser.parse_args()

    # Read parcels table
    conn, cur, df_parcels = get_parcels_table()

    # Isolate parcels that have geolocation data
    df_geo = df_parcels.loc[ df_parcels[LAT].notnull() & df_parcels[LONG].notnull() ]

    # Map parcels to census block groups
    print( '' )
    print( 'Mapping {} geolocations to census block groups'.format( len( df_geo ) ) )
    df_parcels = map_parcels_to_regions( df_parcels, df_geo, get_block_groups_table(), map_point_to_block_group, [GEO_ID, TRACT, BLOCK_GROUP] )
    df_parcels[GEO_ID] = df_parcels[GEO_ID].fillna(0).astype('int64')
    df_parcels[TRACT] = df_parcels[TRACT].fillna(0).astype(int)
    df_parcels[BLOCK_GROUP] = df_parcels[BLOCK_GROUP].fillna(0).astype(int)

    # Map parcels to wards
    print( '' )
    print( 'Mapping {} geolocations to wards'.format( len( df_geo ) ) )
    df_parcels = map_parcels_to_regions( df_parcels, df_geo, get_wards_table(), map_point_to_ward, [WARD, PRECINCT] )

    # Save parcels table
    util.create_table( 'Parcels_L', conn, cur, df=df_parcels )

    util.report_elapsed_time()
