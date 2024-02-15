# Copyright 2024 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import re

from geopy.geocoders import Nominatim
from geopy.geocoders import AzureMaps

from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import shapely.wkt

import os

import sys
sys.path.append('../util')
import util
import normalize
import printctl

LAWRENCE_MIN = 250100
LAWRENCE_MAX = 251899

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

VSID = util.VISION_ID
ADDR = util.NORMALIZED_ADDRESS
LAT = util.LATITUDE
LONG = util.LONGITUDE
GEO_ID = util.CENSUS_GEO_ID
TRACT = util.CENSUS_TRACT
BLOCK_GROUP = util.CENSUS_BLOCK_GROUP

NO_LOCATION = \
{
    LAT: None,
    LONG: None,
}

USER_AGENT = 'City of Lawrence, MA - Office of Energy, Environment, and Sustainability - anil.navkal@CityOfLawrence.com'


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
    conn, cur, engine = util.open_database( '../xl/lawrence/census/us_census_block_group_geometry.sqlite', False )
    df = pd.read_sql_table( 'C2020', engine, columns=BG_COLUMNS )

    # Extract rows pertaining to Lawrence
    df[TRACTCE] = df[TRACTCE].astype( int )
    df = df[ ( df[TRACTCE] >= LAWRENCE_MIN ) & ( df[TRACTCE] <= LAWRENCE_MAX ) ]
    df[TRACTCE] = ( df[TRACTCE] / 100 ).astype( int )

    # Convert polygon string expression to data structure
    for index, row in df.iterrows():
        df.at[index, GEOMETRY] = shapely.wkt.loads( row[GEOMETRY] )

    return df


def load_azure_key():
    with open( '../xl/lawrence/census/azure_key_1.txt' ) as f:
        return f.read()


# Geolocate street address
def geolocate_address( row, geolocator ):

    # Initialize input and output
    street = row[ADDR]
    return_value = NO_LOCATION

    try:
        # Attempt geolocation request
        location = geolocator.geocode( ', '.join( [street, 'LAWRENCE', 'MA'] ).upper() )

        return_value = \
        {
            LAT: location.latitude,
            LONG: location.longitude,
        }

    except Exception as e:

        # Attempt to reformat the address to retry
        retry_street = street

        # Trailing apartment number, e.g. '7 EASTSIDE ST 2' or '202 BROADWAY 2-1' or '11-21 LAWRENCE ST 1'
        if re.match( r'^\d+(\-\d+)* .+ \d+(\-\d+)*$', street ):
            retry_street = re.sub( ' \d+(\-\d+)*$', '', street )

        # Trailing apartment letter, e.g. '74 WOODLAND ST A' or '19 STORROW ST 1A'
        elif re.match( r'^\d+ .+ \d*[A-Z]$', street ):
            retry_street = re.sub( ' \d*[A-Z]$', '', street )

        # Hyphenated street number, e.g. '22-24 WOODLAND CT' or '17-17A WOODLAND ST' or '5-7-7A STEVENS ST' or '36-36A-36B KENDALL ST'
        elif re.match( r'^(\d+[A-Z]*)(\-\d+[A-Z]*)+ ', street ):
            retry_street = re.sub( '^(\d+[A-Z]*)(\-\d+[A-Z]*)+ ', r'\1 ', street )

        # Street number with trailing letter, e.g. '2A SALEM ST'
        elif re.match( r'^(\d+)([A-Z]) ', street ):
            retry_street = re.sub( '^(\d+)([A-Z]) ', r'\1 ', street )

        # If we have a reformatted address, retry
        if street != retry_street:
            print( '   Retry: <{}> -> <{}>'.format( street, retry_street ) )
            retry_row = row.copy()
            retry_row[ADDR] = retry_street
            return_value = geolocate_address( retry_row, geolocator )

    return return_value


def report_unmapped_addresses():
    print( '' )
    print( 'Unmapped addresses: {}'.format( len( df.loc[ df[LAT].isnull() | df[LONG].isnull() ] ) ) )


# Save parcels table and geolocation cache
def save_progress():

    printctl.off()
    util.create_table( output_tablename, conn, cur, df=df )
    printctl.on()

    # Report current status
    report_unmapped_addresses()
    util.report_elapsed_time()


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Test geolocator services for comparison' )
    parser.add_argument( '-s', dest='geo_service_name',  help='Geolocator service name - nominatim or azuremaps', required=True )
    args = parser.parse_args()

    # Initialize geolocator service
    if args.geo_service_name == 'nominatim':
        geo_service = Nominatim( user_agent=USER_AGENT )
        args.geo_service_name = 'Nominatim'
    elif args.geo_service_name == 'azuremaps':
        geo_service = AzureMaps( load_azure_key(), user_agent=USER_AGENT )
        args.geo_service_name = 'AzureMaps'
    else:
        print( 'bad service name {}', args.geo_service_name )
        exit()

    df_bg = get_block_groups_table()

    # Read input: parcels table containing Vision ID and normalized address
    conn, cur, engine = util.open_database( '../db/lawrence_master.sqlite', False )
    df = pd.read_sql_table( 'GeoParcels_L', engine, index_col=util.ID, columns=[VSID,ADDR], parse_dates=True )
    df[LAT] = None
    df[LONG] = None
    df[GEO_ID] = None
    df[TRACT] = None
    df[BLOCK_GROUP] = None

    # Initialize output
    output_filename = './out/geo_' + args.geo_service_name.lower() + '.sqlite'
    conn, cur, engine = util.open_database( output_filename, False )
    output_tablename = 'Geo' + args.geo_service_name

    report_unmapped_addresses()

    n_found = 0
    n_failed = 0

    # Process rows that need geolocation
    for index, row in df.iterrows():

        # Call default geolocator with current address
        geoloc = geolocate_address( row, geo_service )

        # Save non-empty results
        if geoloc != NO_LOCATION:

            # Save geolocation in parcels table
            df.at[index, LAT] = geoloc[LAT]
            df.at[index, LONG] = geoloc[LONG]

            bg = get_block_group( Point( geoloc[LONG], geoloc[LAT] ) )
            if bg != None:
                df.at[index, GEO_ID] = bg[GEO_ID]
                df.at[index, TRACT] = bg[TRACT]
                df.at[index, BLOCK_GROUP] = bg[BLOCK_GROUP]
                geo_id = bg[GEO_ID]
            else:
                geo_id = None

            n_found += 1
            print( '  (+{},-{}) <{}> Found: ({},{},{})'.format( n_found, n_failed, row[ADDR], geoloc[LAT], geoloc[LONG], geo_id ) )

        else:
            n_failed += 1
            print( '  (+{},-{}) <{}> Error'.format( n_found, n_failed, row[ADDR] ) )

        # Save intermediate result
        if ( n_found + 1 ) % 20 == 0:
            save_progress()

    # Save final result
    save_progress()
