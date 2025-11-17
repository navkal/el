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

MBLU = util.MBLU

WARD = util.WARD_NUMBER
PRECINCT = util.PRECINCT_NUMBER

LAWRENCE_MIN = util.LAWRENCE_MIN_BLOCK_GROUP
LAWRENCE_MAX = util.LAWRENCE_MAX_BLOCK_GROUP

LAT = util.LATITUDE
LONG = util.LONGITUDE
ZIP = util.ZIP
GEO = util.GEO_SERVICE

GEO_ID = util.CENSUS_GEO_ID
TRACT = util.CENSUS_TRACT
BLOCK_GROUP = util.CENSUS_BLOCK_GROUP

GEOID = util.GEOID
TRACTCE = util.TRACTCE
BLKGRPCE = util.BLKGRPCE
GEOMETRY = util.GEOMETRY

VSID = util.VISION_ID
ADDRESS = util.ADDRESS

# Transform from projection to lat/long coordinates
TRANSFORMER = Transformer.from_crs( 'epsg:26986', 'epsg:4326', always_xy=True )



def pad_slashes( s_mblu ):

    s_mblu = s_mblu.copy()

    s = ~s_mblu.str.contains( '(?:[^/]*/[^/]*){4,}', regex=True )

    while len( s[s] ) > 0:
        s_mblu.loc[s] = s_mblu.loc[s] + '/'
        s = ~s_mblu.str.contains( '(?:[^/]*/[^/]*){4,}', regex=True )

    # Clear cells that did not need padding
    s = s_mblu.str.contains( '////' )
    s_mblu.loc[s] = ''

    return s_mblu


# Transform polygon from polar to lat/long coordinates
def transform_polygon( poly ):

    # Reorganize current Polygon values into list of tuples
    xx, yy = poly.exterior.coords.xy
    ls_x = xx.tolist()
    ls_y = yy.tolist()
    ls_xy = [ ( ls_x[i], ls_y[i] ) for i in range( 0, len( ls_x ) ) ]

    # Transform coordinates
    ls_lat_long = [ TRANSFORMER.transform( x, y ) for x, y in ls_xy ]

    # Set up return values
    poly = Polygon( ls_lat_long )
    centroid = poly.centroid
    lat = centroid.y
    long = centroid.x

    return poly, centroid, lat, long


# Load parcels geometry dataframe
def load_parcels_geometry():

    # Read raw wards table from the shapefile
    df = gpd.read_file( '../xl/lawrence/geography/parcel_geometry/M149TaxPar_CY23_FY24.shp' )

    # Generate mblu-format column from parcel ID
    df[MBLU] = df['MAP_PAR_ID']
    df[MBLU] = df[MBLU].fillna( '' )
    df[MBLU] = df[MBLU].str.replace( '-', '/' )
    df[MBLU] = df[MBLU].str.replace( '/0/', '//' )
    df[MBLU] = df[MBLU].str.replace( '/0$', '/', regex=True )
    df[MBLU] = df[MBLU].str.replace( '^0/', '/', regex=True )
    df[MBLU] = pad_slashes( df[MBLU] )

    for index, row in df.iterrows():

        # Get geometry for current row
        shape = row[GEOMETRY]

        # If we got a MultiPolygon, take the outer envelope, which will be a Polygon
        if shape.geom_type == 'MultiPolygon':
            poly, centroid, lat, long = transform_polygon( shape.envelope )
            df.at[index, GEOMETRY] = poly
            df.at[index, LAT] = lat
            df.at[index, LONG] = long

        # If we got a Polygon object, take its centroid, which will be a Point
        elif shape.geom_type == 'Polygon':
            poly, centroid, lat, long = transform_polygon( shape )
            df.at[index, GEOMETRY] = centroid
            df.at[index, LAT] = lat
            df.at[index, LONG] = long

    # Return dataframe
    df = df[ [MBLU, GEOMETRY, LAT, LONG] ]
    return df


def load_block_groups_geometry():

    # Read raw block groups table from the shapefile
    df = gpd.read_file( args.block_groups_filename )

    # Extract rows pertaining to Lawrence
    df[TRACTCE] = df[TRACTCE].astype( int )
    df = df[ ( df[TRACTCE] >= LAWRENCE_MIN ) & ( df[TRACTCE] <= LAWRENCE_MAX ) ]
    df[TRACTCE] = ( df[TRACTCE] / 100 ).astype( int )

    return df


def map_location_to_block_group( point, df_bg ):

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


def load_precincts_geometry():

    # Read raw wards table from the shapefile
    df = gpd.read_file( args.wards_filename )

    # Extract rows pertaining to Lawrence
    df = df[ df['TOWN'] == 'LAWRENCE' ]

    for index, row in df.iterrows():

        # Transform to lat/long coordinate system
        poly, centroid, lat, long = transform_polygon( row[GEOMETRY] )

        # Save transformed coordinates in dataframe
        df.at[index, GEOMETRY] = poly

    # Return dataframe
    return df


def map_location_to_precinct( point, df_wp ):

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
    df_manual =  pd.read_excel( '../xl/lawrence/geography/parcel_geolocation_manual_overrides.xlsx' )
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


def map_locations_to_regions( df_parcels, df_geo, df_regions, map_location_to_region, ls_columns ):

    # Initialize new columns
    for col in ls_columns:
        df_parcels[col] = None

    n_found = 0
    n_failed = 0

    # Iterate over parcels that have geolocations
    for index, row in df_geo.iterrows():
        region = map_location_to_region( Point( row[LONG], row[LAT] ), df_regions )
        if region != None:
            n_found += 1
            for col in ls_columns:
                df_parcels.at[index, col] = region[col]
        else:
            n_failed += 1

    print( 'Found {} mappings'.format( n_found ) )

    return df_parcels


def map_geometries_to_regions( df_parcels, df_precincts, df_block_groups ):

    # Get unmapped parcels
    df_unmapped_parcels = df_parcels[ ( df_parcels[GEO_ID] == 0 ) | df_parcels[WARD].isna() ]

    # Get geometries of unmapped parcels
    df_geometries = load_parcels_geometry()
    df_unmapped_geometries = df_geometries[ df_geometries[MBLU].isin( df_unmapped_parcels[MBLU] ) ]

    print( '' )
    print( 'Mapping {} geometries to precincts and census block groups'.format( len( df_unmapped_geometries ) ) )

    n_unmapped_found_in_a_precinct = 0
    n_unmapped_found_in_a_block_group = 0

    for index, row in df_unmapped_geometries.iterrows():

        precinct = map_location_to_precinct( row[GEOMETRY], df_precincts )
        if precinct != None:
            n_unmapped_found_in_a_precinct += 1
            parcels_index = df_parcels[ df_parcels[MBLU] == row[MBLU] ].index[0]
            df_parcels.at[parcels_index, WARD] = precinct[WARD]
            df_parcels.at[parcels_index, PRECINCT] = precinct[PRECINCT]

        block_group = map_location_to_block_group( row[GEOMETRY], df_block_groups )
        if block_group != None:
            n_unmapped_found_in_a_block_group += 1
            parcels_index = df_parcels[ df_parcels[MBLU] == row[MBLU] ].index[0]
            df_parcels.at[parcels_index, GEO_ID] = block_group[GEO_ID]
            df_parcels.at[parcels_index, TRACT] = block_group[TRACT]
            df_parcels.at[parcels_index, BLOCK_GROUP] = block_group[BLOCK_GROUP]

        # If current geometry yielded a new mapping, save lat/long coordinates
        if ( precinct != None ) or ( block_group != None ):
            df_parcels.at[parcels_index, LAT] = row[LAT]
            df_parcels.at[parcels_index, LONG] = row[LONG]
            df_parcels.at[parcels_index, GEO] = 'Geometry'

    print( 'Geometries found in a precinct:', n_unmapped_found_in_a_precinct )
    print( 'Geometries found in a block group:', n_unmapped_found_in_a_block_group )

    df_parcels[LAT] = df_parcels[LAT].astype(float).round( decimals=5 )
    df_parcels[LONG] = df_parcels[LONG].astype(float).round( decimals=5 )
    return df_parcels


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Map parcel geolocations to US Census block groups' )
    parser.add_argument( '-b', dest='block_groups_filename',  help='Input filename - Name of shapefile containing Lawrence block group geometry', required=True )
    parser.add_argument( '-w', dest='wards_filename',  help='Input filename - Name of shapefile containing Lawrence ward geometry', required=True )
    parser.add_argument( '-p', dest='parcels_filename',  help='Input filename - Name of shapefile containing Lawrence parcel geometry', required=True )
    parser.add_argument( '-m', dest='output_filename',  help='Output filename - Name of master database file', required=True )
    args = parser.parse_args()

    # Read region geometries
    df_precincts = load_precincts_geometry()
    df_block_groups = load_block_groups_geometry()

    # Read parcels table
    conn, cur, df_parcels = get_parcels_table()

    # Isolate parcels that have geolocation data
    df_geo = df_parcels.loc[ df_parcels[LAT].notnull() & df_parcels[LONG].notnull() ]

    # Map parcels to precincts
    print( '' )
    print( 'Mapping {} geolocations to precincts'.format( len( df_geo ) ) )
    df_parcels = map_locations_to_regions( df_parcels, df_geo, df_precincts, map_location_to_precinct, [WARD, PRECINCT] )

    # Map parcels to census block groups
    print( '' )
    print( 'Mapping {} geolocations to census block groups'.format( len( df_geo ) ) )
    df_parcels = map_locations_to_regions( df_parcels, df_geo, df_block_groups, map_location_to_block_group, [GEO_ID, TRACT, BLOCK_GROUP] )

    # Map unmapped parcels using parcel geometry
    df_parcels = map_geometries_to_regions( df_parcels, df_precincts, df_block_groups )

    # Fix datatypes
    df_parcels[GEO_ID] = df_parcels[GEO_ID].fillna(0).astype('int64')
    df_parcels[TRACT] = df_parcels[TRACT].fillna(0).astype(int)
    df_parcels[BLOCK_GROUP] = df_parcels[BLOCK_GROUP].fillna(0).astype(int)

    # How did we do?
    df_remaining = df_parcels[ ( df_parcels[GEO_ID] == 0 ) | df_parcels[WARD].isna() ]
    print( 'Remaining unmapped parcels:', len( df_remaining ) )

    # Save parcels table
    util.create_table( 'Parcels_L', conn, cur, df=df_parcels )

    util.report_elapsed_time()
