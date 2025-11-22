# Copyright 2025 Energize Andover.  All rights reserved.

import argparse
import os

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import simplekml

import sys
sys.path.append( '../util' )
import util


# Nicknames
GEOID = util.GEOID
GEOMETRY = util.GEOMETRY
TRACT = util.CENSUS_TRACT
BLOCK_GROUP = util.CENSUS_BLOCK_GROUP
TRACT_DASH_GROUP = util.TRACT_DASH_GROUP
IS_RES = util.IS_RESIDENTIAL
YES = util.YES
WX_PERMIT = util.WX_PERMIT
WX_RATE = 'wx_rate'
WX_SATURATION = 'wx_saturation'


######################


# Generate weatherization rate styles
def make_wx_rate_styles( kml, df_parcels, df_block_groups ):

    doc = kml.newdocument( name="Res Wx Rates" )

    # Prepare dataframe of residential parcels
    df_res = df_parcels.copy()
    df_res = df_res[df_res[IS_RES] == YES]
    df_res[TRACT_DASH_GROUP] = df_res[TRACT].astype(str) + '-' + df_res[BLOCK_GROUP].astype(str)

    # Add weatherization rate column to block groups dataframe
    for idx, row in df_block_groups.copy().iterrows():

        # Find residential parcels in current block group
        df_cbg_res = df_res[df_res[TRACT_DASH_GROUP] == row[TRACT_DASH_GROUP]]

        # Find which residential parcels are weatherized
        df_cbg_wx = df_cbg_res[ ~df_cbg_res[WX_PERMIT].isnull() ]

        # Calculate weatherization rate for current block group
        df_block_groups.at[idx, WX_RATE] = len( df_cbg_wx ) / len( df_cbg_res )

    # Add saturation column to block groups dataframe
    n_max_saturation = 255
    f_min_rate = df_block_groups[WX_RATE].min()
    f_max_rate = df_block_groups[WX_RATE].max()
    f_normalized_rate = f_max_rate - f_min_rate

    for idx, row in df_block_groups.copy().iterrows():
        n_sat = n_max_saturation * ( row[WX_RATE] - f_min_rate ) / f_normalized_rate
        df_block_groups.at[idx, WX_SATURATION] = n_sat
    df_block_groups[WX_SATURATION] = df_block_groups[WX_SATURATION].astype( int )

    # Populate dictionary of block group styles
    s_color = simplekml.Color.rgb( 78, 124, 210 )
    dc_wx_rate_styles = {}
    for idx, row in df_block_groups.iterrows():
        cbg_style = simplekml.Style()
        cbg_style.linestyle.color = simplekml.Color.white
        cbg_style.linestyle.width = 3
        cbg_style.polystyle.fill = 1
        cbg_style.polystyle.color = simplekml.Color.changealphaint( row[WX_SATURATION], s_color )

        # Save style in document and dictionary
        doc.styles.append( cbg_style )
        dc_wx_rate_styles[row[TRACT_DASH_GROUP]] = cbg_style

    return doc, dc_wx_rate_styles


# Generate weatherization rate KML file
def make_wx_rate_kml_file( kml, doc, df_block_groups, dc_wx_rate_styles, output_directory ):

    # Generate polygon for each census block group
    for idx, row in df_block_groups.iterrows():
        s_block_group = row[TRACT_DASH_GROUP]
        n_pct = int( 100 * row[WX_RATE] )
        poly = doc.newpolygon( name=f'{s_block_group}: Res Wx {n_pct}%' )
        poly.outerboundaryis = list( row[GEOMETRY].exterior.coords )
        poly.description = f'<p>Geographic ID: {row[GEOID]}</p><p>Residential Weatherization: {n_pct}%</p>'
        poly.style = dc_wx_rate_styles[s_block_group]

    # Save the KML file
    s_filename = 'res_wx_rates.kml'
    print( '' )
    print( f'Saving weatherization rates KML file "{s_filename}"' )
    kml.save( os.path.join( output_directory, s_filename ) )



######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate KML files showing residential weatherization rates in Lawrence' )
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

    # Create empty KML
    kml = simplekml.Kml()

    # Read the parcels table
    conn, cur, engine = util.open_database( args.master_filename, False )
    print( '' )
    s_table = 'Assessment_L_Parcels'
    print( f'Reading {s_table}' )
    df_parcels = pd.read_sql_table( s_table, engine )

    # Extract block group geometries from shapefile
    df_block_groups = util.get_block_groups_geometry( args.block_groups_filename )

    # Generate weatherization rate styles
    doc, dc_wx_rate_styles = make_wx_rate_styles( kml, df_parcels, df_block_groups )

    # Generate weatherization rate KML file
    make_wx_rate_kml_file( kml, doc, df_block_groups, dc_wx_rate_styles, args.output_directory )

    util.report_elapsed_time()
