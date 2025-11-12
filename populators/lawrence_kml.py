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
        FUEL: [ELEC]
    },
    'res_gas':
    {
        IS_RES: [YES],
        FUEL: [GAS]
    },
    'res_oil':
    {
        IS_RES: [YES],
        FUEL: [OIL]
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
def make_kml_file( city_shape, df, kml_filepath ):

    # Create empty KML
    kml = simplekml.Kml()

    # Set color of boundary and shading
    boundary_color = simplekml.Color.blanchedalmond

    # Generate the boundary and shading
    for geom in city_shape.geometry:
        for subgeom in geom.geoms:
            kml_poly = kml.newpolygon( name='City of Lawrence' )
            kml_poly.outerboundaryis = list( subgeom.exterior.coords )
            kml_poly.style.linestyle.color = boundary_color
            kml_poly.style.linestyle.width = 3
            kml_poly.style.polystyle.color = simplekml.Color.changealphaint( 50, boundary_color )

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

    # Report arguments
    print( '' )
    print( 'Arguments' )
    print( ' Input database:', master_filename )
    print( ' Output directory:', output_directory )

    # Read the parcels table
    conn, cur, engine = util.open_database( master_filename, False )
    print( '' )
    print( f'Reading {TABLE}' )
    df_parcels = pd.read_sql_table( TABLE, engine )

    print( '' )
    print( f'Generating {len( FILTERS ) * len( WX_FILTERS )} KML files' )
    print( '' )

    # Extract Lawrence shape from geodatabase (https://www.mass.gov/info-details/massgis-data-municipalities)
    gdf = gpd.read_file( '../xl/lawrence/geography/city_geometry/townssurvey_gdb/townssurvey.gdb', layer='TOWNSSURVEY_POLYM' )
    city_shape = gdf[gdf['TOWN'].str.strip().str.upper() == 'LAWRENCE'].to_crs( epsg=4326 )

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
            make_kml_file( city_shape, df, filepath )

            # Report progress
            n_files += 1
            print( '{: >3d}: {}'.format( n_files, filename ) )

    return




######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate KML files showing Lawrence parcels partitioned in various ways' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename', required=True )
    parser.add_argument( '-o', dest='output_directory', help='Target directory output files', required=True )
    parser.add_argument( '-c', dest='clear_directory', action='store_true', help='Clear target directory first?' )

    args = parser.parse_args()

    # Optionally clear target directory
    if args.clear_directory:
        util.clear_directory( args.output_directory )

    # Make KML files
    make_kml_files( args.master_filename, args.output_directory )

    util.report_elapsed_time()
