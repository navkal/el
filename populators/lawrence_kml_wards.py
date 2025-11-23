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
WARD = 'WARD'

# Wards
A = util.A
B = util.B
C = util.C
D = util.D
E = util.E
F = util.F

# KML rendering attributes
COLOR = util.COLOR
KML_MAP = util.KML_MAP


######################


# Extract ward geometries from shapefile
def get_wards_geometry( wards_filename ):

    # Extract geometries from shapefile
    df_districts = gpd.read_file( wards_filename )
    df_districts = df_districts[ df_districts['TOWN'] == 'LAWRENCE' ]
    df_districts = df_districts[[WARD,GEOMETRY]]

    # Combine districts into wards
    df_wards = df_districts.dissolve( by=WARD, as_index=False )
    df_wards[GEOMETRY] = df_wards.buffer(0)
    df_wards = df_wards.to_crs( epsg=4326 )

    return df_wards


# Configure per-ward styles
def make_ward_styles( kml, df_wards ):

    doc = kml.newdocument( name='Wards' )
    dc_ward_styles = {}

    for idx, row in df_wards.iterrows():

        # Get current ward
        s_ward = row[WARD]

        # Configure style for current ward
        ward_style = simplekml.Style()
        ward_style.linestyle.color = KML_MAP[COLOR][s_ward]
        ward_style.linestyle.width = 5
        ward_style.polystyle.fill = 1
        ward_style.polystyle.color = '00ffffff'

        # Save style in document and dictionary
        doc.styles.append( ward_style )
        dc_ward_styles[s_ward] = ward_style

    return doc, dc_ward_styles


# Generate wards KML file
def make_wards_kml_file( kml, doc, df_wards, dc_ward_styles, output_directory ):

    # Generate color-coded polygon for each ward
    for idx, row in df_wards.iterrows():
        s_ward = row[WARD]
        s_name = f'Ward {s_ward}'
        poly = doc.newpolygon( name=s_name )
        poly.outerboundaryis = list( row[GEOMETRY].exterior.coords )
        poly.style = dc_ward_styles[s_ward]

    # Save the KML file
    s_filename = 'wards.kml'
    print( '' )
    print( f'Saving wards KML file "{s_filename}"' )
    kml.save( os.path.join( output_directory, s_filename ) )



######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate KML files showing wards in Lawrence' )
    parser.add_argument( '-w', dest='wards_filename',  help='Input filename - Name of shapefile containing Lawrence ward geometry', required=True )
    parser.add_argument( '-o', dest='output_directory', help='Target directory output files', required=True )
    args = parser.parse_args()

    # Report arguments
    print( '' )
    print( 'Arguments' )
    print( ' Wards filename:', args.wards_filename )
    print( ' Output directory:', args.output_directory )

    # Create empty KML
    kml = simplekml.Kml()

    # Extract ward geometries from shapefile
    df_wards = get_wards_geometry( args.wards_filename )

    # Configure per-ward styles
    doc, dc_ward_styles = make_ward_styles( kml, df_wards )

    # Generate wards KML file
    make_wards_kml_file( kml, doc, df_wards, dc_ward_styles, args.output_directory )

    util.report_elapsed_time()
