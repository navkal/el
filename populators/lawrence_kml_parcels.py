# Copyright 2025 Energize Andover.  All rights reserved.

B_DEBUG = False

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
WX = util.WX
NWX = 'n' + WX
COLOR = util.COLOR
ICON = util.ICON
OCC = util.TOTAL_OCCUPANCY
WX_PERMIT = util.WX_PERMIT
IS_RENTAL = 'is_rental'
WARDS=util.LAWRENCE_WARDS
STYLEMAP = 'stylemap'

RES = 'res'
RENT = 'rent'

#
# ==> Dictionary of filters ==>
#

# Each filter corresponds to a single KML output file
FILTERS = \
{
    f'{LEAN}_{ELEC}':
    {
        LEAN_ELIG: [LEAN],
        FUEL: [ELEC],
    },
    f'{LEAN}_{GAS}':
    {
        LEAN_ELIG: [LEAN],
        FUEL: [GAS],
    },
    f'{LEAN}_{OIL}':
    {
        LEAN_ELIG: [LEAN],
        FUEL: [OIL],
    },
    f'{LMF}_{ELEC}':
    {
        LEAN_ELIG: [LMF],
        FUEL: [ELEC],
    },
    f'{LMF}_{GAS}':
    {
        LEAN_ELIG: [LMF],
        FUEL: [GAS],
    },
    f'{LMF}_{OIL}':
    {
        LEAN_ELIG: [LMF],
        FUEL: [OIL],
    },
    f'{RES}_{ELEC}':
    {
        IS_RES: [YES],
        FUEL: [ELEC],
    },
    f'{RES}_{GAS}':
    {
        IS_RES: [YES],
        FUEL: [GAS],
    },
    f'{RES}_{OIL}':
    {
        IS_RES: [YES],
        FUEL: [OIL],
    },
    f'{RENT}_{ELEC}':
    {
        IS_RES: [YES],
        IS_RENTAL: [True],
        FUEL: [ELEC],
    },
    f'{RENT}_{GAS}':
    {
        IS_RES: [YES],
        IS_RENTAL: [True],
        FUEL: [GAS],
    },
    f'{RENT}_{OIL}':
    {
        IS_RES: [YES],
        IS_RENTAL: [True],
        FUEL: [OIL],
    },
}

# Add ward-specific filters

if B_DEBUG:
    FILTERS = \
    {
    }

    WARDS = [util.A]

for ward in WARDS:
    for fuel in [ELEC,GAS,OIL]:
        FILTERS[f'{ward}_{RES}_{fuel}'.lower()] = \
        {
            WARD: [ward],
            IS_RES: [YES],
            FUEL: [fuel],
        }

for ward in WARDS:
    for fuel in [ELEC,GAS,OIL]:
        FILTERS[f'{ward}_{RENT}_{fuel}'.lower()] = \
        {
            WARD: [ward],
            IS_RES: [YES],
            IS_RENTAL: [True],
            FUEL: [fuel],
        }


# Secondary filtering based on weatherization status
WX_FILTERS = \
[
    '',     # No filter
    WX,     # Weatherized
    NWX,    # Not weatherized
]

#
# <== Dictionary of filters <==
#


# KML rendering attributes
KML_MAP = util.KML_MAP

######################


def make_styles():

    df_styles = pd.DataFrame()

    for s_ward in KML_MAP[COLOR]:
        for s_fuel in KML_MAP[ICON]:

            # Extract mapped placemark attributes
            s_color = KML_MAP[COLOR][s_ward]
            icon_colors = s_color.split('|')
            if len( icon_colors ) == 1:
                icon_colors.append( icon_colors[0] )

            s_icon = KML_MAP[ICON][s_fuel]
            shape_scale = s_icon.split('|')

            # Generate normal style
            normal_style = simplekml.Style()
            normal_style.labelstyle.scale = 0
            normal_style.iconstyle.color = icon_colors[0]
            normal_style.iconstyle.icon.href = 'https://maps.google.com/mapfiles/kml/shapes/' + shape_scale[0] + '.png'
            normal_style.iconstyle.scale = shape_scale[1]

            # Generate highlight style for mouseover
            highlight_style = simplekml.Style()
            highlight_style.labelstyle.scale = 1.1
            highlight_style.iconstyle.color = icon_colors[1]

            # Combine normal and highlight styles in a stylemap
            style_map = simplekml.StyleMap()
            style_map.normalstyle = normal_style
            style_map.highlightstyle = highlight_style

            # Save the stylemap in the styles dataframe
            dc_row = \
            {
                WARD: s_ward,
                FUEL: s_fuel,
                STYLEMAP: style_map
            }
            df_styles = df_styles.append( dc_row, ignore_index=True )

    return df_styles


# Generate a KML file containing POIs
def make_kml_file( df, df_styles, s_label, n_parcels, n_units, output_directory ):

    s_docname = make_doc_name( s_label, n_parcels, n_units )
    s_filename = f'{s_label.lower()}.kml'
    s_filepath = os.path.join( output_directory, s_filename )

    # Create empty KML
    kml = simplekml.Kml()
    kml.document.name = s_docname

    for index, row in df.iterrows():

        # Create a point for this parcel
        point = kml.newpoint( name=f'{row[WARD]}: {row[ADDR]}', coords=[ ( row[LONG], row[LAT] ) ] )

        # Set link
        point.description = f'<a href={row[LINK]}>{row[ADDR]}</a><br/>{row[FUEL]} heat'

        # Set the style map to switch between normal and highlight styles
        s_ward = row[WARD]
        s_fuel = row[FUEL]
        style_row = df_styles[ ( df_styles[WARD] == s_ward ) & ( df_styles[FUEL] == s_fuel )]
        style_map = style_row['stylemap'].values[0]
        point.stylemap = style_map

    kml.save( s_filepath )

    return s_filename


# Generate KML files
def make_kml_files( df_parcels, output_directory ):

    print( '' )
    print( f'Generating {len( FILTERS ) * len( WX_FILTERS )} KML files' )
    print( '' )

    # Generate styles of placemarks based on ward and fuel
    df_styles = make_styles()

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
                df = df[ df[WX_PERMIT].isnull() ] if s_wx == NWX else df[ ~df[WX_PERMIT].isnull() ]

            # Count parcels and housing units that will be represented by this KML
            n_parcels = len( df )
            n_units = df[OCC].sum()

            # Edit Vision hyperlinks encoded for Excel
            pattern = r'=HYPERLINK\("(http.*pid=\d+)".*'
            df = df.replace( to_replace=pattern, value=r'\1', regex=True )

            # Reorder columns and rows
            ls_columns = [WARD, STREET_NAME, ADDR, FUEL, LAT, LONG, LINK]
            df = df[ls_columns]
            df = df.sort_values( by=ls_columns )
            df = df.reset_index( drop=True )

            # Convert dataframe to KML
            s_filename = make_kml_file( df, df_styles, s_label, n_parcels, n_units, output_directory )

            # Report progress
            n_files += 1
            print( '{: >3d}: {}'.format( n_files, s_filename ) )

    return


# Generate document name
def make_doc_name( s_label, n_parcels, n_units ):

    ls_in = s_label.split( '_' )

    ls_out = []

    for s_in in ls_in:
        s_out = s_in if s_in in [LEAN, LMF] else s_in.capitalize()
        ls_out.append( s_out )

    s_out = ' '.join( ls_out )

    s_out += f' - P:{n_parcels:,} - H:{n_units:,}'

    return s_out




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
