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


# Name of parcels table
TABLE = 'Assessment_L_Parcels'

# Nicknames
LAWRENCE_MIN = util.LAWRENCE_MIN_BLOCK_GROUP
LAWRENCE_MAX = util.LAWRENCE_MAX_BLOCK_GROUP
GEOID = util.GEOID
TRACTCE = util.TRACTCE
BLKGRPCE = util.BLKGRPCE
GEOMETRY = util.GEOMETRY
TRACT = util.CENSUS_TRACT
BLOCK_GROUP = util.CENSUS_BLOCK_GROUP
ADDR = util.NORMALIZED_ADDRESS
STREET_NAME = util.NORMALIZED_STREET_NAME
IS_RES = util.IS_RESIDENTIAL
FUEL = util.HEATING_FUEL_DESC
WARD = util.WARD_NUMBER
LINK = util.VISION_LINK
LAT = util.LATITUDE
LONG = util.LONGITUDE
ELEC = util.ELECTRIC
OIL = util.OIL
GAS = util.GAS
YES = util.YES
LEAN_ELIG = util.LEAN_ELIGIBILITY
LEAN = util.LEAN
LMF = util.LEAN_MULTI_FAMILY
LABEL = util.LABEL
COLOR = util.COLOR
ICON = util.ICON
OCC = util.TOTAL_OCCUPANCY
WX_PERMIT = util.WX_PERMIT
IS_RENTAL = 'is_rental'


# Wards
A = 'A'
B = 'B'
C = 'C'
D = 'D'
E = 'E'
F = 'F'


#
# ==> Dictionary of filters ==>
#

# Each filter corresponds to a single KML output file
FILTERS = \
{
    'lean_electric':
    {
        LEAN_ELIG: [LEAN],
        FUEL: [ELEC],
    },
    'lean_gas':
    {
        LEAN_ELIG: [LEAN],
        FUEL: [GAS],
    },
    'lean_oil':
    {
        LEAN_ELIG: [LEAN],
        FUEL: [OIL],
    },
    'lmf_electric':
    {
        LEAN_ELIG: [LMF],
        FUEL: [ELEC],
    },
    'lmf_gas':
    {
        LEAN_ELIG: [LMF],
        FUEL: [GAS],
    },
    'lmf_oil':
    {
        LEAN_ELIG: [LMF],
        FUEL: [OIL],
    },
    'res_electric':
    {
        IS_RES: [YES],
        FUEL: [ELEC],
    },
    'res_gas':
    {
        IS_RES: [YES],
        FUEL: [GAS],
    },
    'res_oil':
    {
        IS_RES: [YES],
        FUEL: [OIL],
    },
    'rent_electric':
    {
        IS_RES: [YES],
        FUEL: [ELEC],
        IS_RENTAL: [True],
    },
    'rent_gas':
    {
        IS_RES: [YES],
        FUEL: [GAS],
        IS_RENTAL: [True],
    },
    'rent_oil':
    {
        IS_RES: [YES],
        FUEL: [OIL],
        IS_RENTAL: [True],
    },
}

# Add filters for each ward and fuel
for ward in [A,B,C,D,E,F]:
    for fuel in [ELEC,OIL,GAS]:
        FILTERS[f'{ward}_{fuel}'.lower()] = \
        {
            WARD: [ward],
            FUEL: [fuel],
            LEAN_ELIG: [LEAN, LMF],
        }

# Secondary filtering based on weatherization status
WX_FILTERS = \
[
    '',     # No filter
    'wx',   # Weatherized
    'nwx',  # Not weatherized
]

#
# <== Dictionary of filters <==
#


# Map from column values to pin attributes
dc_map = \
{
    COLOR:
    {
        A: f'{simplekml.Color.red}',
        B: f'{simplekml.Color.orange}',
        C: f'{simplekml.Color.yellow}',
        D: f'{simplekml.Color.chartreuse}',
        E: f'{simplekml.Color.cyan}|{simplekml.Color.green}',
        F: f'{simplekml.Color.hotpink}|{simplekml.Color.purple}',
    },
    ICON:
    {
        ELEC: 'square|0.7',
        OIL: 'placemark_circle|1.1',
        GAS: 'triangle|0.8',
    },
}


######################


# Generate a KML file
def make_kml_file( df, kml_filepath ):

    # Create empty KML
    kml = simplekml.Kml()

    for index, row in df.iterrows():
        point = kml.newpoint( name=f'{row[WARD]}: {row[ADDR]}', coords=[ ( row[LONG], row[LAT] ) ] )

        # Hide the label
        point.style.labelstyle.scale = 0

        # Extract mapped placemark attributes
        shape_scale = str( row[ICON] ).split('|')
        icon_colors = str( row[COLOR] ).split('|')
        if len( icon_colors ) == 1:
            icon_colors.append( icon_colors[0] )

        # Set icon attributes
        point.style.iconstyle.color = icon_colors[0]
        point.style.iconstyle.icon.href = 'https://maps.google.com/mapfiles/kml/shapes/' + shape_scale[0] + '.png'
        point.style.iconstyle.scale = shape_scale[1]

        # Set link
        point.description = f'<a href={row[LINK]}>{row[ADDR]}</a><br/>{row[FUEL]} heat'

        # Create a highlight style for mouseover
        highlight_style = simplekml.Style()
        highlight_style.labelstyle.scale = 1.1
        highlight_style.iconstyle.color = icon_colors[1]

        # Create a style map to switch between normal and highlight styles
        style_map = simplekml.StyleMap()
        style_map.normalstyle = point.style
        style_map.highlightstyle = highlight_style

        point.stylemap = style_map

    kml.save( kml_filepath )

    return


# Generate KML files
def make_kml_files( master_filename, output_directory ):

    # Read the parcels table
    conn, cur, engine = util.open_database( master_filename, False )
    print( '' )
    print( f'Reading {TABLE}' )
    df_parcels = pd.read_sql_table( TABLE, engine )

    print( '' )
    print( f'Generating {len( FILTERS ) * len( WX_FILTERS )} KML files' )
    print( '' )

    # Add rental flag column
    df_parcels[IS_RENTAL] = df_parcels[OCC] > 1

    n_files = 0

    # Generate three KMLs for each filter in the FILTERS table
    for s_filter in FILTERS:

        # Generate three KMLs for current filter
        for s_wx in WX_FILTERS:

            # Start with a copy of the full parcels table
            df = df_parcels.copy()

            # Initialize the label for current KML
            s_label = s_filter

            # Select rows based on current filter
            dc_filters = FILTERS[s_label]
            for col in dc_filters:
                df = df[ df[col].isin( dc_filters[col] ) ]

            # Select further, based on weatherization status
            if s_wx:
                s_label += '_' + s_wx
                df = df[ df[WX_PERMIT].isnull() ] if s_wx == 'nwx' else df[ ~df[WX_PERMIT].isnull() ]

            # Count parcels and housing units that will be represented by this KML
            n_parcels = len( df )
            n_units = df[OCC].sum()

            # Edit Vision hyperlinks encoded for Excel
            pattern = r'=HYPERLINK\("(http.*pid=\d+)".*'
            df = df.replace( to_replace=pattern, value=r'\1', regex=True )

            # Add columns to be replaced with mapped values
            df[COLOR] = df[WARD]
            df[ICON] = df[FUEL]

            # Reorder columns and rows
            ls_columns = [WARD, STREET_NAME, ADDR, FUEL, ICON, LAT, LONG, COLOR, LINK]
            df = df[ls_columns]
            df = df.sort_values( by=ls_columns )
            df = df.reset_index( drop=True )

            # Replace column values with visualization attributes
            for col in dc_map:
                if col in df.columns:
                    df[col] = df[col].replace( dc_map[col] )

            # Convert dataframe to KML
            filename = f'{s_label}_{n_parcels}_{n_units}.kml'
            filepath = os.path.join( output_directory, filename )
            make_kml_file( df, filepath )

            # Report progress
            n_files += 1
            print( '{: >3d}: {}'.format( n_files, filename ) )

    return


# Generate KML file that represents geographical features of the city
def make_geography_file( block_groups_filename, wards_filename, output_directory ):

    # Extract district shapes from shapefile
    df_districts = gpd.read_file( wards_filename )
    df_districts = df_districts[ df_districts['TOWN'] == 'LAWRENCE' ]
    df_districts = df_districts[['WARD',GEOMETRY]]

    # Combine districts into wards
    df_wards = df_districts.dissolve( by='WARD', as_index=False )
    df_wards[GEOMETRY] = df_wards.buffer(0)
    df_wards = df_wards.to_crs( epsg=4326 )

    # Create empty KML
    kml = simplekml.Kml()

    ###################
    # Configure styles
    ###################

    # Configure style for city boundary
    city_style = simplekml.Style()
    city_style.linestyle.color = simplekml.Color.changealphaint( 150, simplekml.Color.cornsilk )
    city_style.linestyle.width = 15
    city_style.polystyle.fill = 1
    city_style.polystyle.color = '00ffffff'

    # Generate per-ward styles
    dc_ward_styles = {}
    doc = kml.newdocument( name="Lawrence" )
    for idx, row in df_wards.iterrows():

        # Map to color for current ward
        s_ward = row['WARD']
        s_color = dc_map[COLOR][s_ward]

        # Configure style for current ward
        ward_style = simplekml.Style()
        ward_style.linestyle.color = s_color
        ward_style.linestyle.width = 5
        ward_style.polystyle.fill = 1
        ward_style.polystyle.color = simplekml.Color.changealphaint( 30, s_color )

        # Save style in document and dictionary
        doc.styles.append( ward_style )
        dc_ward_styles[s_ward] = ward_style

    # Configure style for census block groups
    block_group_style = simplekml.Style()
    block_group_style.linestyle.color = simplekml.Color.whitesmoke
    block_group_style.linestyle.width = 3
    block_group_style.polystyle.fill = 1
    block_group_style.polystyle.color = '00ffffff'
    doc.styles.append( block_group_style )

    ####################
    # Generate features
    ####################

    # Generate city boundary with transparent fill
    df_city = df_districts.dissolve( as_index=False )
    df_city[GEOMETRY] = df_city.buffer(0)
    df_city = df_city.to_crs( epsg=4326 )

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

    # Get census block group data
    df_block_groups = gpd.read_file( block_groups_filename )
    df_block_groups[TRACTCE] = df_block_groups[TRACTCE].astype( int )
    df_block_groups = df_block_groups[ ( df_block_groups[TRACTCE] >= LAWRENCE_MIN ) & ( df_block_groups[TRACTCE] <= LAWRENCE_MAX ) ]
    df_block_groups[BLOCK_GROUP] = ( df_block_groups[TRACTCE] / 100 ).astype(int).astype( str ) + '-' + df_block_groups[BLKGRPCE]
    df_block_groups = df_block_groups[[GEOID, BLOCK_GROUP, GEOMETRY]]
    df_block_groups = df_block_groups.sort_values( by=[GEOID] )

    # Generate polygon for each census block group
    block_group_folder = root_folder.newfolder( name='Census Block Groups' )
    block_group_folder.visibility = 0
    for idx, row in df_block_groups.iterrows():
        poly = block_group_folder.newpolygon( name=f'Block Group {row[BLOCK_GROUP]}' )
        poly.outerboundaryis = list( row[GEOMETRY].exterior.coords )
        poly.description = f'Geographic ID: {row[GEOID]}'
        poly.style = block_group_style


    # Save the KML file
    s_filename = '_geography.kml'
    print( '' )
    print( f'Saving geography file "{s_filename}"' )
    kml.save( os.path.join( output_directory, s_filename ) )



######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate KML files showing Lawrence parcels partitioned in various ways' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename', required=True )
    parser.add_argument( '-b', dest='block_groups_filename',  help='Input filename - Name of shapefile containing Lawrence block group geometry', required=True )
    parser.add_argument( '-w', dest='wards_filename',  help='Input filename - Name of shapefile containing Lawrence ward geometry', required=True )
    parser.add_argument( '-o', dest='output_directory', help='Target directory output files', required=True )
    parser.add_argument( '-c', dest='clear_directory', action='store_true', help='Clear target directory first?' )

    args = parser.parse_args()

    # Report arguments
    print( '' )
    print( 'Arguments' )
    print( ' Input database:', args.master_filename )
    print( ' Wards filename:', args.wards_filename )
    print( ' Output directory:', args.output_directory )

    # Optionally clear target directory
    if args.clear_directory:
        print( ' Clearing output directory' )
        util.clear_directory( args.output_directory )

    # Make geography KML file
    make_geography_file( args.block_groups_filename, args.wards_filename, args.output_directory )

    # Make KML files
    make_kml_files( args.master_filename, args.output_directory )

    util.report_elapsed_time()
