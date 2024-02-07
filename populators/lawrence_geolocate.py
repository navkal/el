# Copyright 2024 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import re

from geopy.geocoders import Nominatim

import os

import sys
sys.path.append('../util')
import util
import normalize


ADDR = util.NORMALIZED_ADDRESS
STREET_NUMBER = util.NORMALIZED_STREET_NUMBER
STREET_NAME = util.NORMALIZED_STREET_NAME
OCCUPANCY = util.NORMALIZED_OCCUPANCY
ADDITIONAL = util.NORMALIZED_ADDITIONAL_INFO

LOCN = util.LOCATION

LAT = util.LATITUDE
LONG = util.LONGITUDE

NO_LOCATION = \
{
    LAT: None,
    LONG: None,
}


n_located = 0
n_failed = 0

# Geolocate street address
def geolocate_address( row, geolocator ):

    global n_located, n_failed

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

        n_located += 1

        print( '  (+{},-{}) <{}> Success: ({},{})'.format( n_located, n_failed, street, return_value[LAT], return_value[LONG] ) )

    except Exception as e:

        # Attempt to reformat the address to retry
        retry_street = street

        # Trailing apartment number, e.g. '7 EASTSIDE ST 2' or '202 BROADWAY 2-1' or '11-21 LAWRENCE ST 1'
        if re.match( r'^\d+(\-\d+)* .+ \d+(\-\d+)*$', street ):
            retry_street = re.sub( ' \d+(\-\d+)*$', '', street )

        # Trailing apartment letter, e.g. '74 WOODLAND ST A' or '19 STORROW ST 1A'
        elif re.match( r'^\d+ .+ \d*[A-Z]$', street ):
            retry_street = re.sub( ' \d*[A-Z]$', '', street )

        # Hyphenated street number, e.g. '22-24 WOODLAND CT' or '17-17A WOODLAND ST'
        elif re.match( r'^(\d+[A-Z]*)\-\d+[A-Z]* ', street ):
            retry_street = re.sub( '^(\d+[A-Z]*)\-\d+[A-Z]* ', r'\1 ', street )

        # Street number with trailing letter, e.g. '2A SALEM ST'
        elif re.match( r'^(\d+)([A-Z]) ', street ):
            retry_street = re.sub( '^(\d+)([A-Z]) ', r'\1 ', street )

        else:
            n_failed += 1

        # If we have a reformatted address, retry
        if street != retry_street:
            print( '  Retry: <{}> -> <{}>'.format( street, retry_street ) )
            retry_row = row.copy()
            retry_row[ADDR] = retry_street
            return_value = geolocate_address( retry_row, geolocator )

        else:
            print( '  (+{},-{}) <{}> Error: {}'.format( n_located, n_failed, street, type( e ).__name__ ) )

    return return_value


def report_unmapped_addresses():
    print( 'Unmapped addresses: {}'.format( len( df_parcels.loc[ df_parcels[LAT].isnull() | df_parcels[LONG].isnull() ] ) ) )



# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Find geolocation coordinates of Lawrence parcel addresses' )
    parser.add_argument( '-p', dest='parcels_filename',  help='Parcels database filename', required=True )
    parser.add_argument( '-g', dest='geo_cache_filename',  help='Geolocation cache filename', required=True )
    args = parser.parse_args()

    # Read parcels data
    conn_parcels, cur_parcels, engine_parcels = util.open_database( args.parcels_filename, False )
    df_parcels = pd.read_sql_table( 'PublishedParcels_L', engine_parcels, index_col=util.ID, parse_dates=True )

    # Read geolocation data
    conn_cache, cur_cache, engine_cache = util.open_database( args.geo_cache_filename, False )
    try:
        df_cache = pd.read_sql_table( 'Geo_Cache_L', engine_cache, index_col=util.ID )
    except:
        df_cache = pd.DataFrame( columns=[ADDR,LAT,LONG] )

    # Normalize parcel addresses
    df_parcels[ADDR] = df_parcels[LOCN]
    df_parcels[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df_parcels.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Merge parcels with coordinates from geolocation cache
    df_parcels = pd.merge( df_parcels, df_cache, how='left', on=[ADDR] )

    # Select rows lacking coordinates
    df_need_geo = df_parcels.loc[ df_parcels[LAT].isnull() | df_parcels[LONG].isnull() ]

    report_unmapped_addresses()

    # Initialize geolocator
    geolocator = Nominatim( user_agent='energize_andover' )

    # Process rows that need geolocation
    for index, row in df_need_geo.iterrows():

        # Call geolocator with current address
        geoloc = geolocate_address( row, geolocator )

        # Save non-empty results
        if geoloc != NO_LOCATION:

            # Save geolocation in parcels table
            print( '--> Updating parcels:', geoloc )
            df_parcels.at[index, LAT] = geoloc[LAT]
            df_parcels.at[index, LONG] = geoloc[LONG]

            # Save new address-to-geolocation mapping to cache
            cache_row = \
            {
                ADDR: row[ADDR],
                LAT: geoloc[LAT],
                LONG: geoloc[LONG]
            }
            print( '--> Saving to cache:', cache_row )
            df_cache = df_cache.append( cache_row, ignore_index=True )

            # Report progress
            report_unmapped_addresses()

    # Save parcels table
    util.create_table( 'Parcels_L', conn_parcels, cur_parcels, df=df_parcels )

    # Save cache
    df_cache = df_cache.sort_values( by=[ADDR] )
    util.create_table( 'Geo_Cache_L', conn_cache, cur_cache, df=df_cache )

    util.report_elapsed_time()
