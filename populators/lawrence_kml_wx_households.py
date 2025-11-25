# Copyright 2025 Energize Andover.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import simplekml

import sys
sys.path.append( '../util' )
import util


# Nicknames
TRACT_DASH_GROUP = util.TRACT_DASH_GROUP
WX_PERMIT = util.WX_PERMIT
HEAT_MAP_VALUE = util.HEAT_MAP_VALUE
OCC = util.TOTAL_OCCUPANCY

# UI labels
HEAT_MAP_NAME = 'Wx of Households'
######################


# Compute heat map values
def compute_heat_map_values( df_heat_map, df_block_groups ):

    # Add weatherization rate column to block groups dataframe
    for idx, row in df_block_groups.copy().iterrows():

        # Find residential parcels in current block group
        df_cbg_res = df_heat_map[df_heat_map[TRACT_DASH_GROUP] == row[TRACT_DASH_GROUP]]

        # Find which residential parcels are weatherized
        df_cbg_wx = df_cbg_res[ ~df_cbg_res[WX_PERMIT].isnull() ]

        # Calculate weatherization rate for current block group
        n_wx = df_cbg_wx[OCC].sum()
        n_res = df_cbg_res[OCC].sum()
        df_block_groups.at[idx, HEAT_MAP_VALUE] = ( 100 * n_wx / n_res ) if n_res != 0 else 0

    return df_block_groups



######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate KML file showing weatherization of households in Lawrence' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename', required=True )
    parser.add_argument( '-b', dest='block_groups_filename',  help='Input filename - Name of shapefile containing Lawrence block group geometry', required=True )
    parser.add_argument( '-o', dest='output_directory', help='Target directory output files', required=True )

    args = parser.parse_args()

    # Report arguments
    print( '' )
    print( 'Arguments' )
    print( ' Input database:', args.master_filename )
    print( ' Block Groups filename:', args.block_groups_filename )
    print( ' Output directory:', args.output_directory )

    # Get dataframe of residential parcels for generating the heat map
    df_res_heat_map = util.get_res_heat_map_data( args.master_filename)

    # Extract block group geometries from shapefile
    df_block_groups = util.get_block_groups_geometry( args.block_groups_filename )

    # Compute heat map values
    df_block_groups = compute_heat_map_values( df_res_heat_map, df_block_groups )

    # Create empty KML
    kml = simplekml.Kml()

    # Generate heat map styles
    doc, dc_heat_map_styles = util.make_heat_map_styles( df_block_groups, kml, HEAT_MAP_NAME )

    # Generate weatherization rate KML file
    util.make_heat_map_kml_file( kml, doc, df_block_groups, HEAT_MAP_NAME, dc_heat_map_styles, args.output_directory, 'wx_households.kml' )

    util.report_elapsed_time()
