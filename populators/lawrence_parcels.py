# Copyright 2024 Energize Lawrence.  All rights reserved.

import os

import sys
sys.path.append( '../util' )
import util

# Main program
if __name__ == '__main__':

    # Add geocoordinates to Lawrence parcel data

    # Read raw parcels data
    print( '\n=======> Parcels input' )
    os.system( 'python db_to_db.py -i ../db/vision_lawrence.sqlite -f Vision_Raw_Lawrence -t Vision_Raw_Lawrence -o ../db/lawrence_parcels.sqlite -c' )

    # Clean parcels data
    print( '\n=======> Parcels table and summary' )
    os.system( 'python vision_clean.py -i Vision_Raw_Lawrence -o Parcels_L -l ../xl/residential_land_use_codes.xlsx -t Lawrence -n -m ../db/lawrence_parcels.sqlite' )

    util.report_elapsed_time()
