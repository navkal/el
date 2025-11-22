# Copyright 2025 Energize Andover.  All rights reserved.

import argparse
import os

import copy

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import simplekml

import sqlite3
import sqlalchemy

import sys
sys.path.append( '../util' )
import util


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
COLOR = util.COLOR
ICON = util.ICON
OCC = util.TOTAL_OCCUPANCY
WX_PERMIT = util.WX_PERMIT
IS_RENTAL = 'is_rental'


# Wards
A = util.A
B = util.B
C = util.C
D = util.D
E = util.E
F = util.F


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


# KML rendering attributes
KML_MAP = util.KML_MAP

######################


# Generate a KML file containing POIs
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
def make_kml_files( df_parcels, output_directory ):

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
            for col in KML_MAP:
                if col in df.columns:
                    df[col] = df[col].replace( KML_MAP[col] )

            # Convert dataframe to KML
            filename = f'{s_label}_{n_parcels}_{n_units}.kml'
            filepath = os.path.join( output_directory, filename )
            make_kml_file( df, filepath )

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

    # Report arguments
    print( '' )
    print( 'Arguments' )
    print( ' Input database:', args.master_filename )
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

    # Make KML files
    make_kml_files( df_parcels, args.output_directory )

    util.report_elapsed_time()
