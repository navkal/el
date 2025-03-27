# Copyright 2025 Energize Andover.  All rights reserved.

import argparse
import os

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

from matplotlib import colors

import csv

import sqlite3
import sqlalchemy

import sys
sys.path.append('')
import util
import csv_to_kml


# Name of parcels table
TABLE = 'Assessment_L_Parcels'

# Nicknames
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
DIAMETER = util.DIAMETER


# Filters for each KML layer
FILTERS = \
{
    'fuel_electric':
    {
        IS_RES: [YES],
        FUEL: [ELEC]
    },
    'fuel_oil':
    {
        IS_RES: [YES],
        FUEL: [OIL]
    },
    'lean_electric':
    {
        IS_RES: [YES],
        LEAN_ELIG: [LEAN],
        FUEL: [ELEC],
    },
    'lean_oil':
    {
        IS_RES: [YES],
        LEAN_ELIG: [LEAN],
        FUEL: [OIL],
    },
    'lmf_electric':
    {
        IS_RES: [YES],
        LEAN_ELIG: [LMF],
        FUEL: [ELEC],
    },
    'lmf_oil':
    {
        IS_RES: [YES],
        LEAN_ELIG: [LMF],
        FUEL: [OIL],
    },
}

# Map from column values to pin attributes
dc_map = \
{
    WARD:
    {
        'A': colors.to_hex( 'red' ),
        'B': colors.to_hex( 'orange' ),
        'C': colors.to_hex( 'yellow' ),
        'D': colors.to_hex( 'blue' ),
        'E': colors.to_hex( 'purple' ),
        'F': colors.to_hex( 'pink' ),
    },
    FUEL:
    {
        ELEC: 'square',
        OIL: 'circle',
        GAS: 'triangle',
    },
}


######################


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
        pattern = r'=HYPERLINK\(("http.*pid=\d+").*'
        df = df.replace( to_replace=pattern, value=r'\1', regex=True )

        # Select columns
        df = df[[FUEL, LAT, LONG, WARD, LINK]]

        # Add columns
        df[LABEL] = df[WARD]
        df[DIAMETER] = 0.00025

        # Reorder columns and rows
        ls_columns = [LABEL, FUEL, LAT, LONG, WARD, DIAMETER, LINK]
        df = df[ls_columns]
        df = df.sort_values( by=ls_columns )
        df = df.reset_index( drop=True )

        #------> Delete this ----------->
        # Save to CSV
        csv_filename = 'DELETE_THIS_' + s_layer + '.csv'
        csv_filepath = os.path.join( output_directory, csv_filename )
        df.to_csv( csv_filepath, index=False, header=True, quoting=csv.QUOTE_NONE )
        #<------ Delete this <-----------

        # Replace column values with visualization attributes
        for col in dc_map:
            if col in df.columns:
                df[col] = df[col].replace( dc_map[col] )

        # Save to CSV
        csv_filepath = make_filepath( output_directory, s_layer, 'csv' )
        df.to_csv( csv_filepath, index=False, header=False, quoting=csv.QUOTE_NONE )

        # Convert CSV to KML
        print( 'Generating KML' )
        kml_filepath = make_filepath( output_directory, s_layer, 'kml' )
        csv_to_kml.create_kml( csv_filepath, kml_filepath )

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
