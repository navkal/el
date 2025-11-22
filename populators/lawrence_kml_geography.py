# Copyright 2025 Energize Andover.  All rights reserved.

import argparse
import os

import copy

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import geopandas as gpd
import simplekml

import sqlite3
import sqlalchemy

import sys
sys.path.append( '../util' )
import util


# Nicknames
LAWRENCE_MIN = util.LAWRENCE_MIN_BLOCK_GROUP
LAWRENCE_MAX = util.LAWRENCE_MAX_BLOCK_GROUP
GEOID = util.GEOID
TRACTCE = util.TRACTCE
BLKGRPCE = util.BLKGRPCE
GEOMETRY = util.GEOMETRY
TRACT = util.CENSUS_TRACT
BLOCK_GROUP = util.CENSUS_BLOCK_GROUP
TRACT_DASH_GROUP = 'tract_dash_group'
IS_RES = util.IS_RESIDENTIAL
WARD = util.WARD_NUMBER

YES = util.YES
COLOR = util.COLOR
OCC = util.TOTAL_OCCUPANCY
WX_PERMIT = util.WX_PERMIT
WX_RATE = 'wx_rate'
WX_SATURATION = 'wx_saturation'


# Wards
A = util.A
B = util.B
C = util.C
D = util.D
E = util.E
F = util.F

# KML rendering attributes
KML_MAP = util.KML_MAP


######################


# Extract geometries from shapefile
def get_kml_geometry( wards_filename, block_groups_filename ):

    # Extract geometries from shapefile
    df_districts = gpd.read_file( wards_filename )
    df_districts = df_districts[ df_districts['TOWN'] == 'LAWRENCE' ]
    df_districts = df_districts[['WARD',GEOMETRY]]

    # Combine districts into city
    df_city = df_districts.dissolve( as_index=False )
    df_city[GEOMETRY] = df_city.buffer(0)
    df_city = df_city.to_crs( epsg=4326 )

    # Combine districts into wards
    df_wards = df_districts.dissolve( by='WARD', as_index=False )
    df_wards[GEOMETRY] = df_wards.buffer(0)
    df_wards = df_wards.to_crs( epsg=4326 )

    # Get census block group data
    df_block_groups = gpd.read_file( block_groups_filename )
    df_block_groups[TRACTCE] = df_block_groups[TRACTCE].astype( int )
    df_block_groups = df_block_groups[ ( df_block_groups[TRACTCE] >= LAWRENCE_MIN ) & ( df_block_groups[TRACTCE] <= LAWRENCE_MAX ) ]
    df_block_groups[TRACT_DASH_GROUP] = ( df_block_groups[TRACTCE] / 100 ).astype(int).astype( str ) + '-' + df_block_groups[BLKGRPCE]
    df_block_groups = df_block_groups[[GEOID, TRACT_DASH_GROUP, GEOMETRY]]
    df_block_groups = df_block_groups.sort_values( by=[GEOID] )

    return df_city, df_wards, df_block_groups


# Configure style for city boundary
def make_city_style( doc ):
    city_style = simplekml.Style()
    city_style.linestyle.color = simplekml.Color.changealphaint( 150, simplekml.Color.cornsilk )
    city_style.linestyle.width = 15
    city_style.polystyle.fill = 1
    city_style.polystyle.color = '00ffffff'
    doc.styles.append( city_style )
    return city_style


# Generate per-ward styles
def make_ward_styles( df_wards, doc ):

    dc_ward_styles = {}

    for idx, row in df_wards.iterrows():

        # Map to color for current ward
        s_ward = row['WARD']
        s_color = KML_MAP[COLOR][s_ward]

        # Configure style for current ward
        ward_style = simplekml.Style()
        ward_style.linestyle.color = s_color
        ward_style.linestyle.width = 5
        ward_style.polystyle.fill = 1
        ward_style.polystyle.color = simplekml.Color.changealphaint( 30, s_color )

        # Save style in document and dictionary
        doc.styles.append( ward_style )
        dc_ward_styles[s_ward] = ward_style

    return dc_ward_styles


# Generate weatherization rate styles
def make_wx_rate_styles( df_parcels, df_block_groups, doc ):

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

    return dc_wx_rate_styles


# Configure geography styles
def make_geography_styles( kml, df_wards, df_parcels, df_block_groups ):

    doc = kml.newdocument( name="Lawrence" )

    city_style = make_city_style( doc )

    # Configure style for city boundary
    city_style = make_city_style( doc )

    # Generate per-ward styles
    dc_ward_styles = make_ward_styles( df_wards, doc )

    # Generate per-block-group styles
    dc_wx_rate_styles = make_wx_rate_styles( df_parcels, df_block_groups, doc )

    return city_style, dc_ward_styles, dc_wx_rate_styles


# Generate KML file that represents geographical features of the city
def make_geography_kml_file( df_parcels, wards_filename, block_groups_filename, output_directory ):

    # Extract geometries from shapefile
    df_city, df_wards, df_block_groups = get_kml_geometry( wards_filename, block_groups_filename )

    # Create empty KML
    kml = simplekml.Kml()

    # Configure styles
    city_style, dc_ward_styles, dc_wx_rate_styles = make_geography_styles( kml, df_wards, df_parcels, df_block_groups )

    # Generate city boundary with transparent fill
    root_folder = kml.newfolder( name='Geography' )
    poly = root_folder.newpolygon( name='City of Lawrence' )
    poly.outerboundaryis = list( df_city.iloc[0][GEOMETRY].exterior.coords )
    poly.style = city_style

    # Generate color-coded polygon for each ward
    ward_folder = root_folder.newfolder( name='Wards' )
    for idx, row in df_wards.iterrows():
        s_ward = row['WARD']
        s_name = f'Ward {s_ward}'
        poly = ward_folder.newpolygon( name=s_name )
        poly.outerboundaryis = list( row[GEOMETRY].exterior.coords )
        poly.style = dc_ward_styles[s_ward]

    # Generate polygon for each census block group
    wx_rate_folder = root_folder.newfolder( name='Residential Weatherization' )
    wx_rate_folder.visibility = 0
    for idx, row in df_block_groups.iterrows():
        s_block_group = row[TRACT_DASH_GROUP]
        n_pct = int( 100 * row[WX_RATE] )
        poly = wx_rate_folder.newpolygon( name=f'{s_block_group}: Res Wx {n_pct}%' )
        poly.outerboundaryis = list( row[GEOMETRY].exterior.coords )
        poly.description = f'<p>Geographic ID: {row[GEOID]}</p><p>Residential Weatherization: {n_pct}%</p>'
        poly.style = dc_wx_rate_styles[s_block_group]

    # Save the KML file
    s_filename = '_geography.kml'
    print( '' )
    print( f'Saving geography file "{s_filename}"' )
    kml.save( os.path.join( output_directory, s_filename ) )



######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate KML files showing Lawrence geography' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename', required=True )
    parser.add_argument( '-w', dest='wards_filename',  help='Input filename - Name of shapefile containing Lawrence ward geometry', required=True )
    parser.add_argument( '-b', dest='block_groups_filename',  help='Input filename - Name of shapefile containing Lawrence block group geometry', required=True )
    parser.add_argument( '-o', dest='output_directory', help='Target directory output files', required=True )
    parser.add_argument( '-c', dest='clear_directory', action='store_true', help='Clear target directory first?' )

    args = parser.parse_args()

    # Report arguments
    print( '' )
    print( 'Arguments' )
    print( ' Input database:', args.master_filename )
    print( ' Wards filename:', args.wards_filename )
    print( ' Block Groups filename:', args.block_groups_filename )
    print( ' Output directory:', args.output_directory )

    # Optionally clear target directory
    if args.clear_directory:
        print( ' Clearing output directory' )
        util.clear_directory( args.output_directory )

    # Read the parcels table
    conn, cur, engine = util.open_database( args.master_filename, False )
    print( '' )
    s_table = 'Assessment_L_Parcels'
    print( f'Reading {s_table}' )
    df_parcels = pd.read_sql_table( s_table, engine )

    # Make geography KML file
    make_geography_kml_file( df_parcels, args.wards_filename, args.block_groups_filename, args.output_directory )

    util.report_elapsed_time()
