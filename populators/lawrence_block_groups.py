# Copyright 2024 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import shapely.wkt

import os

import sys
sys.path.append('../util')
import util


LAWRENCE_MIN = 250100
LAWRENCE_MAX = 251899

LAT = util.LATITUDE
LONG = util.LONGITUDE
GEO_ID = util.CENSUS_GEO_ID
TRACT = util.CENSUS_TRACT
BLOCK_GROUP = util.CENSUS_BLOCK_GROUP

GEOID = 'GEOID'
TRACTCE = 'TRACTCE'
BLKGRPCE = 'BLKGRPCE'
GEOMETRY = 'geometry'

BG_COLUMNS = \
[
    GEOID,
    TRACTCE,
    BLKGRPCE,
    GEOMETRY,
]


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

    # Get the block groups table
    conn, cur, engine = util.open_database( args.block_groups_filename, False )
    df = pd.read_sql_table( 'C2020', engine, columns=BG_COLUMNS )

    # Extract rows pertaining to Lawrence
    df[TRACTCE] = df[TRACTCE].astype( int )
    df = df[ (df[TRACTCE] >= 250100) & (df[TRACTCE] <= 251899) ]
    df[TRACTCE] = ( df[TRACTCE] / 100 ).astype( int )

    # Convert polygon string expression to data structure
    for index, row in df.iterrows():
        df.at[index, GEOMETRY] = shapely.wkt.loads( row[GEOMETRY] )

    return df


def get_parcels_table():

    # Get the parcels table
    conn, cur, engine = util.open_database( args.output_filename, False )
    df = pd.read_sql_table( 'GeoParcels_L', engine, index_col=util.ID )

    # Initialize new columns
    df[TRACT] = None
    df[BLOCK_GROUP] = None

    return conn, cur, df


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Map parcel geolocations to US Census block groups' )
    parser.add_argument( '-b', dest='block_groups_filename',  help='Block group database filename ', required=True )
    parser.add_argument( '-o', dest='output_filename',  help='Output filename - Name of master database file', required=True )
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


    # Save to database
    df_parcels[GEO_ID] = df_parcels[GEO_ID].fillna(0).astype('int64')
    df_parcels[TRACT] = df_parcels[TRACT].fillna(0).astype(int)
    df_parcels[BLOCK_GROUP] = df_parcels[BLOCK_GROUP].fillna(0).astype(int)
    util.create_table( 'Parcels_L', conn, cur, df=df_parcels )

    util.report_elapsed_time()
