# Copyright 2024 Energize Lawrence.  All rights reserved.

import os

import sys
sys.path.append( '../util' )
import util

# Main program
if __name__ == '__main__':

    # Add geolocation to Lawrence parcel data and preserve mappings in geolocation cache

    # Copy scraped and cleaned Lawrence parcels data to parcels database
    print( '\n=======> Copy published Lawrence scrape' )
    os.system( 'python db_to_db.py -i ../db/vision_master.sqlite -f Vision_Lawrence -t Vision_Lawrence -o ../db/lawrence_parcels.sqlite -c' )

    # Generate parcels table with normalized addresses and geolocation data
    print( '\n=======> Generate parcels table with geolocation' )
    os.system( 'python lawrence_geolocate.py -p ../db/lawrence_parcels.sqlite -g ../db/lawrence_geo_cache.sqlite' )

    util.report_elapsed_time()
