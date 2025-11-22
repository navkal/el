# Copyright 2025 Energize Andover.  All rights reserved.

import argparse
import os

import geopandas as gpd
import simplekml

import sys
sys.path.append( '../util' )
import util


# Nicknames
GEOMETRY = util.GEOMETRY





######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate KML file showing Lawrence city boundary' )
    parser.add_argument( '-w', dest='wards_filename',  help='Input filename - Name of shapefile containing Lawrence ward geometry', required=True )
    parser.add_argument( '-o', dest='output_directory', help='Target directory output files', required=True )

    args = parser.parse_args()

    # Report arguments
    print( '' )
    print( 'Arguments' )
    print( ' Wards filename:', args.wards_filename )
    print( ' Output directory:', args.output_directory )

    # Extract geometries from shapefile
    df_districts = gpd.read_file( args.wards_filename )
    df_districts = df_districts[ df_districts['TOWN'] == 'LAWRENCE' ]

    # Combine districts into city
    df_city = df_districts.dissolve( as_index=False )
    df_city[GEOMETRY] = df_city.buffer(0)
    df_city = df_city.to_crs( epsg=4326 )

    # Configure style
    city_style = simplekml.Style()
    city_style.linestyle.color = simplekml.Color.changealphaint( 150, simplekml.Color.cornsilk )
    city_style.linestyle.width = 15
    city_style.polystyle.fill = 1
    city_style.polystyle.color = '00ffffff'

    # Generate the polygon
    kml = simplekml.Kml()
    poly = kml.newpolygon( name='City of Lawrence' )
    poly.outerboundaryis = list( df_city.iloc[0][GEOMETRY].exterior.coords )
    poly.style = city_style

    # Save the KML file
    s_filename = 'city.kml'
    print( '' )
    print( f'Saving city KML file "{s_filename}"' )
    kml.save( os.path.join( args.output_directory, s_filename ) )

    util.report_elapsed_time()
