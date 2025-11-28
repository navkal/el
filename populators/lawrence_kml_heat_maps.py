# Copyright 2025 Energize Andover.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import simplekml

import os

import sys
sys.path.append( '../util' )
import util


# Nicknames
LAWRENCE_BLUE = simplekml.Color.rgb( 78, 124, 210 )

GEOID = util.GEOID
TRACT_DASH_GROUP = util.TRACT_DASH_GROUP
OCC = util.TOTAL_OCCUPANCY
FUEL = util.HEATING_FUEL_DESC
ELEC = util.ELECTRIC
OIL = util.OIL
WX_PERMIT = util.WX_PERMIT

HEAT_MAP_LABEL = 'heat_map_label'
HEAT_MAP_PREFIX = 'heat_map_prefix'
HEAT_MAP_UNIT = 'heat_map_unit'
HEAT_MAP_VALUE = 'heat_map_value'
HEAT_MAP_OPACITY = 'heat_map_opacity'



# Heat map names
ELEC_OIL_HOUSEHOLDS = 'elec_oil_households'
WX_HOUSEHOLDS_PCT = 'wx_households_pct'
WX_RES_PARCELS_PCT = 'wx_res_parcels_pct'

POPULATION = util.POPULATION
HOUSEHOLDS = util.HOUSEHOLDS
POV_ = 'poverty_'
POV_POPULATION = POV_ + POPULATION
POV_HOUSEHOLDS = POV_ + HOUSEHOLDS
POV_HOUSEHOLDS_PCT = POV_HOUSEHOLDS + '_pct'
MED_INCOME = 'median_household_income'


# Dictionary of heat maps to be generated
dc_heat_maps = \
{
    ELEC_OIL_HOUSEHOLDS:
    {
        HEAT_MAP_LABEL: 'Electric and Oil Households',
        HEAT_MAP_PREFIX: '',
        HEAT_MAP_UNIT: '',
    },
    WX_HOUSEHOLDS_PCT:
    {
        HEAT_MAP_LABEL: 'Wx of Households',
        HEAT_MAP_PREFIX: '',
        HEAT_MAP_UNIT: '%',
    },
    WX_RES_PARCELS_PCT:
    {
        HEAT_MAP_LABEL: 'Wx of Res Parcels',
        HEAT_MAP_PREFIX: '',
        HEAT_MAP_UNIT: '%',
    },
    POPULATION:
    {
        HEAT_MAP_LABEL: 'Population',
        HEAT_MAP_PREFIX: '',
        HEAT_MAP_UNIT: '',
    },
    HOUSEHOLDS:
    {
        HEAT_MAP_LABEL: 'Households',
        HEAT_MAP_PREFIX: '',
        HEAT_MAP_UNIT: '',
    },
    POV_POPULATION:
    {
        HEAT_MAP_LABEL: 'Poverty Population',
        HEAT_MAP_PREFIX: '',
        HEAT_MAP_UNIT: '',
    },
    POV_HOUSEHOLDS:
    {
        HEAT_MAP_LABEL: 'Poverty Households',
        HEAT_MAP_PREFIX: '',
        HEAT_MAP_UNIT: '',
    },
    POV_HOUSEHOLDS_PCT:
    {
        HEAT_MAP_LABEL: 'Percent Poverty Households',
        HEAT_MAP_PREFIX: '',
        HEAT_MAP_UNIT: '%',
    },
    MED_INCOME:
    {
        HEAT_MAP_LABEL: 'Median Household Income',
        HEAT_MAP_PREFIX: '$',
        HEAT_MAP_UNIT: '',
    },
}

######################


# Prepare dataframe of block group statistics for use in heat map generation
def get_block_group_stats( census_data_filename ):

    df_stats = pd.read_csv( census_data_filename )
    df_stats[POV_HOUSEHOLDS_PCT] = round( 100 * df_stats[POV_HOUSEHOLDS] / df_stats[HOUSEHOLDS] )
    df_stats[POV_HOUSEHOLDS_PCT] = df_stats[POV_HOUSEHOLDS_PCT].astype( int )
    df_stats[GEOID] = df_stats[GEOID].astype(str)
    return df_stats


# Prepare dataframe of residential parcels for use in heat map generation
def get_res_parcels( engine ):

    # Read the parcels table
    print( '' )
    s_table = 'Assessment_L_Parcels'
    print( f'Reading {s_table}' )
    print( '' )
    df_parcels = pd.read_sql_table( s_table, engine )

    # Prepare dataframe of residential parcels
    df_res_parcels = df_parcels.copy()
    df_res_parcels = df_res_parcels[df_res_parcels[util.IS_RESIDENTIAL] == util.YES]

    # Add census block group labeling
    df_res_parcels[TRACT_DASH_GROUP] = df_res_parcels[util.CENSUS_TRACT].astype(str) + '-' + df_res_parcels[util.CENSUS_BLOCK_GROUP].astype(str)

    return df_res_parcels


# --------------------------------------------
# --> Functions to compute heat map values -->
# --------------------------------------------

class Compute:


    def elec_oil_households( df_res_parcels, df_stats, df_block_groups, s_name ):

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


    def wx_households_pct( df_res_parcels, df_stats, df_block_groups, s_name ):

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


    def wx_res_parcels_pct( df_res_parcels, df_stats, df_block_groups, s_name ):

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


    def population( df_res_parcels, df_stats, df_block_groups, s_name ):
        return make_heatmap_from_stats( df_stats, df_block_groups, s_name )

    def households( df_res_parcels, df_stats, df_block_groups, s_name ):
        return make_heatmap_from_stats( df_stats, df_block_groups, s_name )

    def poverty_population( df_res_parcels, df_stats, df_block_groups, s_name ):
        return make_heatmap_from_stats( df_stats, df_block_groups, s_name )

    def poverty_households( df_res_parcels, df_stats, df_block_groups, s_name ):
        return make_heatmap_from_stats( df_stats, df_block_groups, s_name )

    def poverty_households_pct( df_res_parcels, df_stats, df_block_groups, s_name ):
        return make_heatmap_from_stats( df_stats, df_block_groups, s_name )

    def median_household_income( df_res_parcels, df_stats, df_block_groups, s_name ):
        return make_heatmap_from_stats( df_stats, df_block_groups, s_name )


def make_heatmap_from_stats( df_stats, df_block_groups, s_name ):

    # Add column of heat map values to block groups dataframe
    for idx, row in df_block_groups.copy().iterrows():

        # Copy value from census stats
        sr_geoid = df_stats[df_stats[GEOID] == row[GEOID]]
        df_block_groups.at[idx, HEAT_MAP_VALUE] = sr_geoid[s_name]

    df_block_groups[HEAT_MAP_VALUE] = df_block_groups[HEAT_MAP_VALUE].astype( int )
    df_block_groups[s_name] = df_block_groups[HEAT_MAP_VALUE]

    return df_block_groups


# --------------------------------------------
# <-- Functions to compute heat map values <--
# --------------------------------------------


# Generate heat map styles
def make_heat_map_styles( df_block_groups, kml, s_label ):

    # Add saturation column to block groups dataframe
    n_max_saturation = 255
    f_min_rate = df_block_groups[HEAT_MAP_VALUE].min()
    f_max_rate = df_block_groups[HEAT_MAP_VALUE].max()
    f_normalized_rate = f_max_rate - f_min_rate

    for idx, row in df_block_groups.copy().iterrows():
        n_sat = n_max_saturation * ( row[HEAT_MAP_VALUE] - f_min_rate ) / f_normalized_rate
        df_block_groups.at[idx, HEAT_MAP_OPACITY] = n_sat
    df_block_groups[HEAT_MAP_OPACITY] = df_block_groups[HEAT_MAP_OPACITY].astype( int )

    # Populate dictionary of block group styles
    s_color = LAWRENCE_BLUE
    dc_heat_map_styles = {}
    doc = kml.newdocument( name=s_label )
    for idx, row in df_block_groups.iterrows():
        cbg_style = simplekml.Style()
        cbg_style.linestyle.color = simplekml.Color.white
        cbg_style.linestyle.width = 3
        cbg_style.polystyle.fill = 1
        cbg_style.polystyle.color = simplekml.Color.changealphaint( row[HEAT_MAP_OPACITY], s_color )

        # Save style in document and dictionary
        doc.styles.append( cbg_style )
        dc_heat_map_styles[row[TRACT_DASH_GROUP]] = cbg_style

    return doc, dc_heat_map_styles


# Generate KML heat map of data partitioned by census block groups
def make_heat_map_kml_file( kml, doc, df_block_groups, s_prefix, s_unit, s_label, dc_heat_map_styles, output_directory, s_name ):

    # Enhance document name
    n_min = df_block_groups[HEAT_MAP_VALUE].min()
    n_max = df_block_groups[HEAT_MAP_VALUE].max()
    s_min = f'{s_prefix}{n_min:,}{s_unit}'
    s_max = f'{s_prefix}{n_max:,}{s_unit}'
    s_to = ' - ' if ( s_prefix or s_unit ) else '-'
    s_range = f': {s_min}{s_to}{s_max}'
    doc.name += s_range

    # Generate polygon for each census block group
    for idx, row in df_block_groups.iterrows():
        s_block_group = row[TRACT_DASH_GROUP]
        n_value = row[HEAT_MAP_VALUE]
        s_value = f'{s_prefix}{n_value:,}{s_unit}'
        poly = doc.newpolygon( name=f'{s_block_group}: {s_value}' )
        poly.outerboundaryis = list( row[util.GEOMETRY].exterior.coords )
        poly.description = f'<p>Geographic ID: {row[GEOID]}</p><p>{s_label}: {s_value}</p>'
        poly.style = dc_heat_map_styles[s_block_group]

    # Save the KML file
    s_saving_to = f'{s_name}.kml'
    print( f'Saving "{s_label}" KML file "{s_saving_to}"' )
    kml.save( os.path.join( output_directory, s_saving_to ) )



######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate KML heat map showing electric- and oil-heated households in Lawrence' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename', required=True )
    parser.add_argument( '-b', dest='block_groups_filename',  help='Input filename - Name of shapefile containing Lawrence block group geometry', required=True )
    parser.add_argument( '-c', dest='census_data_filename',  help='Input filename - Name of CSV file containing US Census data partitioned by block groups', required=True )
    parser.add_argument( '-o', dest='output_directory', help='Target directory output files', required=True )

    args = parser.parse_args()

    # Report arguments
    print( '' )
    print( 'Arguments' )
    print( ' Input database:', args.master_filename )
    print( ' Block Groups filename:', args.block_groups_filename )
    print( ' Census DAta filename:', args.census_data_filename )
    print( ' Output directory:', args.output_directory )

    # Open the database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Extract dataframe of residential parcels for generating the heat map
    df_res_parcels = get_res_parcels( engine )

    # Extract dataframe of residential parcels for generating the heat map
    df_stats = get_block_group_stats( args.census_data_filename )
    # print( df_stats )
    # util.exit()

    # Extract block group geometries from shapefile
    df_block_groups = util.get_block_groups_geometry( args.block_groups_filename )

    # Generate heat maps
    for s_name in dc_heat_maps:

        # Get attributes of current heat map
        dc = dc_heat_maps[s_name]
        s_label = dc[HEAT_MAP_LABEL]
        s_prefix = dc[HEAT_MAP_PREFIX]
        s_unit = dc[HEAT_MAP_UNIT]

        # Compute heat map values
        f = getattr( Compute, s_name )
        df_block_groups = f( df_res_parcels, df_stats, df_block_groups, s_name )

        # Create empty KML
        kml = simplekml.Kml()

        # Generate heat map styles
        doc, dc_heat_map_styles = make_heat_map_styles( df_block_groups, kml, s_label )

        # Generate KML heat map file
        make_heat_map_kml_file( kml, doc, df_block_groups, s_prefix, s_unit, s_label, dc_heat_map_styles, args.output_directory, s_name )


    # Save heat map values to database
    df_block_groups = df_block_groups.drop( columns=[util.GEOMETRY, HEAT_MAP_VALUE, HEAT_MAP_OPACITY] )
    util.create_table( 'HeatMaps_L', conn, cur, df=df_block_groups )

    util.report_elapsed_time()
