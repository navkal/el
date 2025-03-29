# Copyright 2025 Energize Andover.  All rights reserved.

import argparse
import os

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import simplekml

import csv

import sqlite3
import sqlalchemy

import sys
sys.path.append('')
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

A = 'A'
B = 'B'
C = 'C'
D = 'D'
E = 'E'
F = 'F'




# Filters for each KML layer
FILTERS = \
{
    'lean_electric':
    {
        LEAN_ELIG: [LEAN],
        FUEL: [ELEC],
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
        print( f'{ward}_{fuel}'.lower() )
        print( FILTERS[f'{ward}_{fuel}'.lower()] )
        

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
        OIL: 'placemark_circle|1',
        GAS: 'star|0.9',
    },
}


######################

def make_klm_layer( df, kml_filepath ):

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
        point.description = f'<a href={row[LINK]}>{row[ADDR]}</a>'

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



def make_filepath( output_directory, s_layer, s_ext ):
    filename = s_layer + '.' + s_ext
    filepath = os.path.join( output_directory, filename )
    return filepath


# Generate KML layers, each represented by an intermediate CSV file and a KML result file
def make_kml_layers(  input_filename, output_directory ):

    # Report arguments
    print( '' )
    print( 'Arguments' )
    print( '  Input database:', input_filename )
    print( '  Output directory:', output_directory )

    # Read the parcels table
    conn, cur, engine = util.open_database( input_filename, False )
    print( '' )
    print( f'Reading {TABLE}' )
    df_parcels = pd.read_sql_table( TABLE, engine )

    for s_layer in FILTERS:

        df = df_parcels.copy()

        # Select rows based on specified filters
        dc_filters = FILTERS[s_layer]
        for col in dc_filters:
            df = df[ df[col].isin( dc_filters[col] ) ]

        print( '' )
        print( f'Layer "{s_layer}" contains {len(df)} parcels' )

        # Edit Vision hyperlinks encoded for Excel
        pattern = r'=HYPERLINK\("(http.*pid=\d+)".*'
        df = df.replace( to_replace=pattern, value=r'\1', regex=True )

        # Add columns to be replaced with mapped values
        df[COLOR] = df[WARD]
        df[ICON] = df[FUEL]

        # Reorder columns and rows
        ls_columns = [WARD, STREET_NAME, ADDR, ICON, LAT, LONG, COLOR, LINK]
        df = df[ls_columns]
        df = df.sort_values( by=ls_columns )
        df = df.reset_index( drop=True )

        # Replace column values with visualization attributes
        for col in dc_map:
            if col in df.columns:
                df[col] = df[col].replace( dc_map[col] )

        # Convert CSV to KML
        print( 'Generating KML' )
        make_klm_layer( df, make_filepath( output_directory, s_layer, 'kml' ) )

    print( '' )
    print( 'Done' )
    util.exit()


######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate KML layers, each represented by an intermediate CSV file and a KML result file' )
    parser.add_argument( '-i', dest='input_filename', help='Input filename - Name of SQLite database file', required=True )
    parser.add_argument( '-o', dest='output_directory', help='Target directory output files', required=True )
    args = parser.parse_args()

    make_kml_layers( args.input_filename, args.output_directory )
