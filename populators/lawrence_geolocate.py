# Copyright 2024 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import re

from geopy.geocoders import Nominatim
from geopy.geocoders import AzureMaps

import os

import sys
sys.path.append('../util')
import util
import normalize
import printctl


ADDR = util.NORMALIZED_ADDRESS
STREET_NUMBER = util.NORMALIZED_STREET_NUMBER
STREET_NAME = util.NORMALIZED_STREET_NAME
OCCUPANCY = util.NORMALIZED_OCCUPANCY
ADDITIONAL = util.NORMALIZED_ADDITIONAL_INFO

LOCN = util.LOCATION

LAT = util.LATITUDE
LONG = util.LONGITUDE
ZIP = util.ZIP

NO_LOCATION = \
{
    LAT: None,
    LONG: None,
    ZIP: None,
}

GEO = util.GEO_SERVICE



def load_azure_key():
    with open( '../xl/lawrence/census/azure_key_1.txt' ) as f:
        return f.read()

USER_AGENT = 'City of Lawrence, MA - Office of Energy, Environment, and Sustainability - anil.navkal@CityOfLawrence.com'
GEO_SERVICE_AZURE = AzureMaps( load_azure_key(), user_agent=USER_AGENT )
GEO_SERVICE_NOMINATIM = Nominatim( user_agent=USER_AGENT )

LAWRENCE_ZIPS = ['01840','01841','01842','01843']


# Validate geolocation based on returned zip code
def validate_geo( d_addr, street_name ):
    zip = d_addr['postalCode'] if 'postalCode' in d_addr else d_addr['postcode']
    valid_zip = zip if zip in LAWRENCE_ZIPS else None
    return valid_zip


# Geolocate street address
def geolocate_address( row, geolocator, addressdetails=None ):

    # Initialize input and output
    street = row[ADDR]
    return_value = NO_LOCATION

    try:
        # Attempt geolocation request
        request = ', '.join( [street, 'LAWRENCE', 'MA'] ).upper()
        if addressdetails:
            location = geolocator.geocode( request, addressdetails=addressdetails )
        else:
            location = geolocator.geocode( request )

        # Validate returned location
        zip = validate_geo( location.raw['address'], row[STREET_NAME] )
        if zip:
            return_value = \
            {
                LAT: location.latitude,
                LONG: location.longitude,
                ZIP: zip,
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
            return_value = geolocate_address( retry_row, geolocator, addressdetails )

    return return_value


def report_unmapped_addresses():
    print( '' )
    print( 'Unmapped addresses: {}'.format( len( df_parcels.loc[ df_parcels[LAT].isnull() | df_parcels[LONG].isnull() | df_parcels[ZIP].isnull() ] ) ) )


# Save parcels table and geolocation cache
def save_progress():

    global df_cache

    # Clear meaningless excess precision
    df_parcels[LAT] = df_parcels[LAT].astype(float).round( decimals=5 )
    df_parcels[LONG] = df_parcels[LONG].astype(float).round( decimals=5 )
    df_cache[LAT] = df_cache[LAT].astype(float).round( decimals=5 )
    df_cache[LONG] = df_cache[LONG].astype(float).round( decimals=5 )

    # Prepare to save
    df_cache = df_cache.drop_duplicates( subset=[ADDR], keep='last' )
    df_cache = df_cache.sort_values( by=[ADDR] )

    # Save parcels table and cache
    printctl.off()
    util.create_table( 'GeoParcels_L', conn_parcels, cur_parcels, df=df_parcels )
    util.create_table( 'GeoCache_L', conn_cache, cur_cache, df=df_cache )
    printctl.on()

    # Report current status
    report_unmapped_addresses()
    util.report_elapsed_time()


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
        df_cache = pd.read_sql_table( 'GeoCache_L', engine_cache, index_col=util.ID )
    except:
        df_cache = pd.DataFrame( columns=[ADDR,LAT,LONG,ZIP,GEO] )

    # Normalize parcel addresses
    df_parcels[ADDR] = df_parcels[LOCN]
    df_parcels[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df_parcels.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Merge parcels with coordinates from geolocation cache
    df_parcels = pd.merge( df_parcels, df_cache, how='left', on=[ADDR] )

    # Select rows with null coordinates and addresses that begin with digits
    df_need_geo = df_parcels.loc[ ( df_parcels[LAT].isnull() | df_parcels[LONG].isnull() | df_parcels[ZIP].isnull() ) & df_parcels[ADDR].str[0].str.isdigit() ]

    report_unmapped_addresses()

    n_found = 0
    n_failed = 0

    # Process rows that need geolocation
    for index, row in df_need_geo.iterrows():

        # Call default geolocator with current address
        geo_service = 'Primary'
        geoloc = geolocate_address( row, GEO_SERVICE_AZURE )

        # If first geolocator failed, try again with alternate geolocator
        if geoloc == NO_LOCATION:
            geo_service = 'Secondary'
            geoloc = geolocate_address( row, GEO_SERVICE_NOMINATIM, addressdetails=True )

        # Save non-empty results
        if geoloc != NO_LOCATION:

            # Save result in parcels table
            df_parcels.at[index, LAT] = geoloc[LAT]
            df_parcels.at[index, LONG] = geoloc[LONG]
            df_parcels.at[index, ZIP] = geoloc[ZIP]
            df_parcels.at[index, GEO] = geo_service

            # Save result in cache
            cache_row = \
            {
                ADDR: row[ADDR],
                LAT: geoloc[LAT],
                LONG: geoloc[LONG],
                ZIP: geoloc[ZIP],
                GEO: geo_service,
            }
            df_cache = df_cache.append( cache_row, ignore_index=True )

            n_found += 1
            print( '  (+{},-{}) <{}> Found: ({},{},{},{})'.format( n_found, n_failed, row[ADDR], geoloc[LAT], geoloc[LONG], geoloc[ZIP], geo_service ) )

        else:
            n_failed += 1
            print( '  (+{},-{}) <{}> Error'.format( n_found, n_failed, row[ADDR] ) )

        # Save intermediate result
        if ( n_found + 1 ) % 20 == 0:
            save_progress()

    # Save final result
    save_progress()
