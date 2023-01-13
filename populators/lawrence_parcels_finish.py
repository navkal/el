# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse
import os
import pandas as pd

import sys
sys.path.append( '../util' )
import util


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Process data on Lawrence parcels, scraped from Vision database' )
    parser.add_argument( '-d', dest='database_filename',  help='Output filename - Name of parcels database file', required=True )
    args = parser.parse_args()

    # Post-process scraped parcel records
    print( '\n=======> Post-process' )
    os.system( 'python lawrence_parcels.py -p -d {0}'.format( args.database_filename ) )

    # Summarize scraped parcel records
    print( '\n=======> Summarize' )
    os.system( 'python lawrence_parcels.py -s -d {0}'.format( args.database_filename ) )

    # Generate copyright notice
    print( '\n=======> Copyright' )
    df_about = pd.DataFrame( columns=['copyright'], data=['© 2023 Energize Lawrence.  All rights reserved.'] )
    util.create_about_table( 'LawrenceParcels', df_about, args.database_filename )

    # Report elapsed time
    util.report_elapsed_time()
