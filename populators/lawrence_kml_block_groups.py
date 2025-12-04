# Copyright 2025 Energize Andover.  All rights reserved.

import argparse
import os

import simplekml

import sys
sys.path.append( '../util' )
import util


# Nicknames
GEOID = util.GEOID
GEOMETRY = util.GEOMETRY
TRACT_DASH_GROUP = util.TRACT_DASH_GROUP

######################


# Configure style for block group boundaries
def make_block_group_style():
    block_group_style = simplekml.Style()
    block_group_style.linestyle.color = simplekml.Color.white
    block_group_style.linestyle.width = 4
    block_group_style.polystyle.fill = 1
    block_group_style.polystyle.color = '00ffffff'
    return block_group_style


# Generate KML file representing Lawrence census block groups
def make_block_groups_kml_file( df_block_groups, block_group_style, output_directory ):

    # Create KML with empty folder
    kml = simplekml.Kml()
    block_groups_folder = kml.newfolder( name='Census Block Groups' )

    # Generate polygon for each census block group
    for idx, row in df_block_groups.iterrows():
        s_block_group = row[TRACT_DASH_GROUP]
        poly = block_groups_folder.newpolygon( name=s_block_group )
        poly.outerboundaryis = list( row[GEOMETRY].exterior.coords )
        poly.description = f'<p>Geographic ID: {row[GEOID]}</p>'
        poly.style = block_group_style

    # Save the KML file
    s_filename = 'block_groups.kml'
    print( '' )
    print( f'Saving block groups KML file "{s_filename}"' )
    kml.save( os.path.join( output_directory, s_filename ) )


######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate a KML file showing census block groups in Lawrence' )
    parser.add_argument( '-b', dest='block_groups_filename',  help='Input filename - Name of shapefile containing Lawrence block group geometry', required=True )
    parser.add_argument( '-o', dest='output_directory', help='Target directory output files', required=True )
    args = parser.parse_args()

    # Report arguments
    print( '' )
    print( 'Arguments' )
    print( ' Block Groups filename:', args.block_groups_filename )
    print( ' Output directory:', args.output_directory )

    # Extract block group geometries from shapefile
    df_block_groups = util.get_block_groups_geometry( args.block_groups_filename )

    # Configure block group style
    block_group_style = make_block_group_style()

    # Generate KML file representing Lawrence census block groups
    make_block_groups_kml_file( df_block_groups, block_group_style, args.output_directory )

    util.report_elapsed_time()
