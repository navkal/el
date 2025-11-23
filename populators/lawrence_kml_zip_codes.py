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
POSTCODE = 'POSTCODE'



######################


# Extract ZIP code geometries from shapefile
def get_zip_codes_geometry( zip_codes_filename ):

    # Extract geometries from shapefile
    df_zip_codes = gpd.read_file( zip_codes_filename )
    df_zip_codes = df_zip_codes[ df_zip_codes['CITY_TOWN'] == 'LAWRENCE' ]
    df_zip_codes = df_zip_codes[[POSTCODE, GEOMETRY]]
    df_zip_codes[GEOMETRY] = df_zip_codes.buffer(0)
    df_zip_codes = df_zip_codes.to_crs( epsg=4326 )

    return df_zip_codes


# Configure style for zip code boundaries
def make_zip_code_style():
    zip_code_style = simplekml.Style()
    zip_code_style.linestyle.color = simplekml.Color.rgb( 255, 212, 128 )
    zip_code_style.linestyle.width = 6
    zip_code_style.polystyle.fill = 1
    zip_code_style.polystyle.color = '00ffffff'
    return zip_code_style


# Generate KML file representing Lawrence zip codes
def make_zip_codes_kml_file( df_zip_codes, zip_code_style, output_directory ):

    # Create KML with empty folder
    kml = simplekml.Kml()
    zip_codes_folder = kml.newfolder( name='ZIP Codes' )

    # Generate polygon for each ZIP code
    for idx, row in df_zip_codes.iterrows():
        s_zip_code = row[POSTCODE]
        poly = zip_codes_folder.newpolygon( name=s_zip_code )
        poly.outerboundaryis = list( row[GEOMETRY].exterior.coords )
        poly.description = f'<p>ZIP Code: {row[POSTCODE]}</p>'
        poly.style = zip_code_style

    # Save the KML file
    s_filename = 'zip_codes.kml'
    print( '' )
    print( f'Saving ZIP Codes KML file "{s_filename}"' )
    kml.save( os.path.join( output_directory, s_filename ) )



######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate KML files showing wards in Lawrence' )
    parser.add_argument( '-z', dest='zip_codes_filename',  help='Input filename - Name of shapefile containing Lawrence ZIP Codes geometry', required=True )
    parser.add_argument( '-o', dest='output_directory', help='Target directory output files', required=True )
    args = parser.parse_args()

    # Report arguments
    print( '' )
    print( 'Arguments' )
    print( ' ZIP Codes filename:', args.zip_codes_filename )
    print( ' Output directory:', args.output_directory )

    # Create empty KML
    kml = simplekml.Kml()

    # Extract ZIP codes geometries from shapefile
    df_zip_codes = get_zip_codes_geometry( args.zip_codes_filename )

    # Configure zip code style
    zip_code_style = make_zip_code_style()

    # Generate KML file representing Lawrence zip codes
    make_zip_codes_kml_file( df_zip_codes, zip_code_style, args.output_directory )

    util.report_elapsed_time()
