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
OCC = util.TOTAL_OCCUPANCY
FUEL = util.HEATING_FUEL_DESC
ELEC = util.ELECTRIC
OIL = util.OIL
WX_PERMIT = util.WX_PERMIT

# UI labels
ELEC_OIL_HOUSEHOLDS = 'elec_oil_households'
WX_HOUSEHOLDS_PCT = 'wx_households_pct'
WX_RES_PARCELS_PCT = 'wx_res_parcels_pct'

HEAT_MAP_LABEL = util.HEAT_MAP_LABEL
HEAT_MAP_VALUE = util.HEAT_MAP_VALUE
HEAT_MAP_UNIT = util.HEAT_MAP_UNIT

# Dictionary of heat maps to be generated
dc_heat_maps = \
{
    ELEC_OIL_HOUSEHOLDS:
    {
        HEAT_MAP_LABEL: 'Electric and Oil Households',
        HEAT_MAP_UNIT: '',
    },
    WX_HOUSEHOLDS_PCT:
    {
        HEAT_MAP_LABEL: 'Wx of Households',
        HEAT_MAP_UNIT: '%',
    },
    WX_RES_PARCELS_PCT:
    {
        HEAT_MAP_LABEL: 'Wx of Res Parcels',
        HEAT_MAP_UNIT: '%',
    },
}

######################



# --------------------------------------------
# --> Functions to compute heat map values -->
# --------------------------------------------

class Compute:


    def elec_oil_households( df_res_parcels, df_block_groups, s_name ):

        # Add column of heat map values to block groups dataframe
        for idx, row in df_block_groups.copy().iterrows():

            # Find residential parcels in current block group
            df_cbg_res = df_res_parcels[df_res_parcels[TRACT_DASH_GROUP] == row[TRACT_DASH_GROUP]]

            # Isolate residential parcels with either electric or oil heat
            df_cbg_elec_oil = df_cbg_res[ df_cbg_res[FUEL].isin( [ELEC, OIL] ) ]

            # Count households in current block group with electric or oil heat
            n_households = df_cbg_elec_oil[OCC].sum()
            df_block_groups.at[idx, HEAT_MAP_VALUE] = n_households

        df_block_groups[HEAT_MAP_VALUE] = df_block_groups[HEAT_MAP_VALUE].astype( int )
        df_block_groups[s_name] = df_block_groups[HEAT_MAP_VALUE]

        return df_block_groups


    def wx_households_pct( df_res_parcels, df_block_groups, s_name ):

        # Add column of heat map values to block groups dataframe
        for idx, row in df_block_groups.copy().iterrows():

            # Find residential parcels in current block group
            df_cbg_res = df_res_parcels[df_res_parcels[TRACT_DASH_GROUP] == row[TRACT_DASH_GROUP]]

            # Find which residential parcels are weatherized
            df_cbg_wx = df_cbg_res[ ~df_cbg_res[WX_PERMIT].isnull() ]

            # Calculate weatherization rate for current block group
            n_wx = df_cbg_wx[OCC].sum()
            n_res = df_cbg_res[OCC].sum()
            df_block_groups.at[idx, HEAT_MAP_VALUE] = ( 100 * n_wx / n_res ) if n_res != 0 else 0

        df_block_groups[HEAT_MAP_VALUE] = df_block_groups[HEAT_MAP_VALUE].astype( int )
        df_block_groups[s_name] = df_block_groups[HEAT_MAP_VALUE]

        return df_block_groups


    def wx_res_parcels_pct( df_res_parcels, df_block_groups, s_name ):

        # Add column of heat map values to block groups dataframe
        for idx, row in df_block_groups.copy().iterrows():

            # Find residential parcels in current block group
            df_cbg_res = df_res_parcels[df_res_parcels[TRACT_DASH_GROUP] == row[TRACT_DASH_GROUP]]

            # Find which residential parcels are weatherized
            df_cbg_wx = df_cbg_res[ ~df_cbg_res[WX_PERMIT].isnull() ]

            # Calculate weatherization rate for current block group
            n_wx = len( df_cbg_wx )
            n_res = len( df_cbg_res )
            df_block_groups.at[idx, HEAT_MAP_VALUE] = ( 100 * n_wx / n_res ) if n_res != 0 else 0

        df_block_groups[HEAT_MAP_VALUE] = df_block_groups[HEAT_MAP_VALUE].astype( int )
        df_block_groups[s_name] = df_block_groups[HEAT_MAP_VALUE]

        return df_block_groups


# --------------------------------------------
# <-- Functions to compute heat map values <--
# --------------------------------------------



######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate KML heat map showing electric- and oil-heated households in Lawrence' )
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

    # Read the parcels table
    conn, cur, engine = util.open_database( args.master_filename, False )
    print( '' )
    s_table = 'Assessment_L_Parcels'
    print( f'Reading {s_table}' )
    df_parcels = pd.read_sql_table( s_table, engine )

    # Extract dataframe of residential parcels for generating the heat map
    df_res_parcels = util.get_res_heat_map_data( df_parcels )

    # Extract block group geometries from shapefile
    df_block_groups = util.get_block_groups_geometry( args.block_groups_filename )

    # Generate heat maps
    for s_name in dc_heat_maps:

        # Get attributes of current heat map
        dc = dc_heat_maps[s_name]
        s_label = dc[HEAT_MAP_LABEL]
        s_unit = dc[HEAT_MAP_UNIT]

        # Compute heat map values
        f = getattr( Compute, s_name )
        df_block_groups = f( df_res_parcels, df_block_groups, s_name )

        # Create empty KML
        kml = simplekml.Kml()

        # Generate heat map styles
        doc, dc_heat_map_styles = util.make_heat_map_styles( df_block_groups, kml, s_label )

        # Generate KML heat map file
        util.make_heat_map_kml_file( kml, doc, df_block_groups, s_unit, s_label, dc_heat_map_styles, args.output_directory, s_name )


    # Save heat map values to database
    df_block_groups = df_block_groups.drop( columns=[util.GEOMETRY, util.HEAT_MAP_VALUE, util.HEAT_MAP_OPACITY] )
    util.create_table( 'HeatMaps_L', conn, cur, df=df_block_groups )

    util.report_elapsed_time()
