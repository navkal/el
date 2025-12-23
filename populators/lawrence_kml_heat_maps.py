# Copyright 2025 Energize Andover.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import simplekml
import numpy as np

import xml.etree.ElementTree as ET
import uuid

import os

import sys
sys.path.append( '../util' )
import util


KML_NAMESPACE = util.KML_NAMESPACE


# Nicknames
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
HEAT_MAP_VISIBILITY = 'heat_map_visibility'
HEAT_MAP_VALUE = 'heat_map_value'
SPECTRUM_INDEX = 'spectrum_index'


# Generate a color spectrumm represented as a list of RGB tuples
def make_spectrum( ls_rbg_start, ls_rbg_end, n_colors ):

    # Set start and end colors
    start_color = np.array( ls_rbg_start )
    end_color = np.array( ls_rbg_end )

    # Generate spectrum of rgb colors and convert from numpy to native python types
    ls_spectrum_np = np.linspace( ls_rbg_start, end_color, num=n_colors, dtype=int )
    ls_spectrum = [tuple(color) for color in ls_spectrum_np]

    return ls_spectrum

SPECTRUM_GRAY = [200, 200, 200]
SPECTRUM_BLUE = [0, 50, 255]
SPECTRUM_LEN = 256
HEAT_MAP_SPECTRUM = make_spectrum( SPECTRUM_GRAY, SPECTRUM_BLUE, SPECTRUM_LEN )

# Heat map names
HEALTH = 'health'
HEALTH_RISK_SCORE = util.HEALTH_RISK_SCORE
HOUSEHOLDS = 'households'
HOUSEHOLDS_ELEC_OIL = 'households_elec_oil'
HOUSEHOLDS_ELEC_OIL_NWX = 'households_elec_oil_nwx'
HOUSEHOLDS_ELEC_OIL_WX = 'households_elec_oil_wx'
HOUSEHOLDS_MEDIAN_INCOME = 'households_median_income'
HOUSEHOLDS_NWX = 'households_nwx'
HOUSEHOLDS_POVERTY = 'households_poverty'
HOUSEHOLDS_POVERTY_PCT = 'households_poverty_pct'
HOUSEHOLDS_WX = 'households_wx'
HOUSEHOLDS_WX_PCT = 'households_wx_pct'
POPULATION = 'population'
POPULATION_POVERTY = 'population_poverty'
RES_PARCELS_NWX = 'res_parcels_nwx'
RES_PARCELS_WX = 'res_parcels_wx'
RES_PARCELS_WX_PCT = 'res_parcels_wx_pct'

XML = 'xml'
KML = 'kml'


# Dictionary of heat maps to be generated
DC_HEAT_MAPS = \
{
    HEALTH_RISK_SCORE:
    {
        HEAT_MAP_LABEL: '# Health - Risk Score',
        HEAT_MAP_PREFIX: '#',
        HEAT_MAP_UNIT: '',
        HEAT_MAP_VISIBILITY: 0,
    },
    HOUSEHOLDS:
    {
        HEAT_MAP_LABEL: '# Households',
        HEAT_MAP_PREFIX: '#',
        HEAT_MAP_UNIT: '',
        HEAT_MAP_VISIBILITY: 0,
    },
    HOUSEHOLDS_ELEC_OIL:
    {
        HEAT_MAP_LABEL: '# Households - Elec & Oil',
        HEAT_MAP_PREFIX: '#',
        HEAT_MAP_UNIT: '',
        HEAT_MAP_VISIBILITY: 0,
    },
    HOUSEHOLDS_ELEC_OIL_NWX:
    {
        HEAT_MAP_LABEL: '# Households - Elec & Oil (Nwx)',
        HEAT_MAP_PREFIX: '#',
        HEAT_MAP_UNIT: '',
        HEAT_MAP_VISIBILITY: 0,
    },
    HOUSEHOLDS_ELEC_OIL_WX:
    {
        HEAT_MAP_LABEL: '# Households - Elec & Oil (Wx)',
        HEAT_MAP_PREFIX: '#',
        HEAT_MAP_UNIT: '',
        HEAT_MAP_VISIBILITY: 0,
    },
    HOUSEHOLDS_MEDIAN_INCOME:
    {
        HEAT_MAP_LABEL: '$ Households - Median Income',
        HEAT_MAP_PREFIX: '$',
        HEAT_MAP_UNIT: '',
        HEAT_MAP_VISIBILITY: 1,
    },
    HOUSEHOLDS_NWX:
    {
        HEAT_MAP_LABEL: '# Households (Nwx)',
        HEAT_MAP_PREFIX: '#',
        HEAT_MAP_UNIT: '',
        HEAT_MAP_VISIBILITY: 0,
    },
    HOUSEHOLDS_POVERTY:
    {
        HEAT_MAP_LABEL: '# Households - Poverty',
        HEAT_MAP_PREFIX: '#',
        HEAT_MAP_UNIT: '',
        HEAT_MAP_VISIBILITY: 0,
    },
    HOUSEHOLDS_POVERTY_PCT:
    {
        HEAT_MAP_LABEL: '% Households - Poverty',
        HEAT_MAP_PREFIX: '',
        HEAT_MAP_UNIT: '%',
        HEAT_MAP_VISIBILITY: 0,
    },
    HOUSEHOLDS_WX:
    {
        HEAT_MAP_LABEL: '# Households (Wx)',
        HEAT_MAP_PREFIX: '#',
        HEAT_MAP_UNIT: '',
        HEAT_MAP_VISIBILITY: 0,
    },
    HOUSEHOLDS_WX_PCT:
    {
        HEAT_MAP_LABEL: '% Households (Wx)',
        HEAT_MAP_PREFIX: '',
        HEAT_MAP_UNIT: '%',
        HEAT_MAP_VISIBILITY: 0,
    },
    POPULATION:
    {
        HEAT_MAP_LABEL: '# Population',
        HEAT_MAP_PREFIX: '#',
        HEAT_MAP_UNIT: '',
        HEAT_MAP_VISIBILITY: 0,
    },
    POPULATION_POVERTY:
    {
        HEAT_MAP_LABEL: '# Population - Poverty',
        HEAT_MAP_PREFIX: '#',
        HEAT_MAP_UNIT: '',
        HEAT_MAP_VISIBILITY: 0,
    },
    RES_PARCELS_NWX:
    {
        HEAT_MAP_LABEL: '# Res Parcels (Nwx)',
        HEAT_MAP_PREFIX: '#',
        HEAT_MAP_UNIT: '',
        HEAT_MAP_VISIBILITY: 0,
    },
    RES_PARCELS_WX:
    {
        HEAT_MAP_LABEL: '# Res Parcels (Wx)',
        HEAT_MAP_PREFIX: '#',
        HEAT_MAP_UNIT: '',
        HEAT_MAP_VISIBILITY: 0,
    },
    RES_PARCELS_WX_PCT:
    {
        HEAT_MAP_LABEL: '% Res Parcels (Wx)',
        HEAT_MAP_PREFIX: '',
        HEAT_MAP_UNIT: '%',
        HEAT_MAP_VISIBILITY: 0,
    },
}


FOLDER_LABEL = 'folder_label'
FOLDER_CONTENTS = 'folder_contents'

# Demographics folders
POPULATION_FOLDER = 'demo_population_folder'
HOUSEHOLDS_FOLDER = 'demo_households_folder'
HEALTH_FOLDER = 'demo_health_folder'

# Heating fuel folders
HEATING_FUEL_FOLDER = 'heating_fuel_folder'

# Weatherization folders
WX_HOUSEHOLDS_FOLDER = 'wx_households_folder'
WX_PARCELS_FOLDER = 'wx_parcels_folder'

# Dictionary of folders for grouping heat maps
DC_FOLDERS = \
{
    # Demographics
    POPULATION_FOLDER: \
    {
        FOLDER_LABEL: 'Population',
        FOLDER_CONTENTS: [POPULATION, POPULATION_POVERTY],
    },
    HOUSEHOLDS_FOLDER: \
    {
        FOLDER_LABEL: 'Households',
        FOLDER_CONTENTS: [HOUSEHOLDS, HOUSEHOLDS_MEDIAN_INCOME, HOUSEHOLDS_POVERTY, HOUSEHOLDS_POVERTY_PCT],
    },
    HEALTH_FOLDER: \
    {
        FOLDER_LABEL: 'Health',
        FOLDER_CONTENTS: [HEALTH_RISK_SCORE],
    },

    # Heating Fuel
    HEATING_FUEL_FOLDER: \
    {
        FOLDER_LABEL: 'Households Fuel',
        FOLDER_CONTENTS: [HOUSEHOLDS_ELEC_OIL, HOUSEHOLDS_ELEC_OIL_NWX, HOUSEHOLDS_ELEC_OIL_WX],
    },

    # Weatherization
    WX_HOUSEHOLDS_FOLDER: \
    {
        FOLDER_LABEL: 'Households Weatherization',
        FOLDER_CONTENTS: [HOUSEHOLDS_NWX, HOUSEHOLDS_WX, HOUSEHOLDS_WX_PCT],
    },
    WX_PARCELS_FOLDER: \
    {
        FOLDER_LABEL: 'Parcels Weatherization',
        FOLDER_CONTENTS: [RES_PARCELS_NWX, RES_PARCELS_WX, RES_PARCELS_WX_PCT],
    },
}


# --------------------------------------------
# --> Functions to compute heat map values -->
# --------------------------------------------

class Compute:


    def households( df_res_parcels, df_stats, df_block_groups, s_name ):
        return make_heat_map_from_stats( df_stats, df_block_groups, s_name )


    def households_elec_oil( df_res_parcels, df_stats, df_block_groups, s_name ):

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


    def households_elec_oil_nwx( df_res_parcels, df_stats, df_block_groups, s_name ):

        # Add column of heat map values to block groups dataframe
        for idx, row in df_block_groups.copy().iterrows():

            # Find residential parcels in current block group
            df_cbg_res = df_res_parcels[df_res_parcels[TRACT_DASH_GROUP] == row[TRACT_DASH_GROUP]]

            # Isolate residential parcels with either electric or oil heat
            df_cbg_elec_oil = df_cbg_res[ df_cbg_res[FUEL].isin( [ELEC, OIL] ) ]

            # Find which electric and oil parcels are not weatherized
            df_cbg_elec_oil_nwx = df_cbg_elec_oil[ df_cbg_elec_oil[WX_PERMIT].isnull() ]

            # Count households in current block group with electric or oil heat and no weatherization
            n_households = df_cbg_elec_oil_nwx[OCC].sum()
            df_block_groups.at[idx, HEAT_MAP_VALUE] = n_households

        df_block_groups[HEAT_MAP_VALUE] = df_block_groups[HEAT_MAP_VALUE].astype( int )
        df_block_groups[s_name] = df_block_groups[HEAT_MAP_VALUE]

        return df_block_groups


    def households_elec_oil_wx( df_res_parcels, df_stats, df_block_groups, s_name ):

        # Add column of heat map values to block groups dataframe
        for idx, row in df_block_groups.copy().iterrows():

            # Find residential parcels in current block group
            df_cbg_res = df_res_parcels[df_res_parcels[TRACT_DASH_GROUP] == row[TRACT_DASH_GROUP]]

            # Isolate residential parcels with either electric or oil heat
            df_cbg_elec_oil = df_cbg_res[ df_cbg_res[FUEL].isin( [ELEC, OIL] ) ]

            # Find which electric and oil parcels are weatherized
            df_cbg_elec_oil_wx = df_cbg_elec_oil[ ~df_cbg_elec_oil[WX_PERMIT].isnull() ]

            # Count households in current block group with electric or oil heat and weatherization
            n_households = df_cbg_elec_oil_wx[OCC].sum()
            df_block_groups.at[idx, HEAT_MAP_VALUE] = n_households

        df_block_groups[HEAT_MAP_VALUE] = df_block_groups[HEAT_MAP_VALUE].astype( int )
        df_block_groups[s_name] = df_block_groups[HEAT_MAP_VALUE]

        return df_block_groups


    def households_median_income( df_res_parcels, df_stats, df_block_groups, s_name ):
        return make_heat_map_from_stats( df_stats, df_block_groups, s_name )


    def households_nwx( df_res_parcels, df_stats, df_block_groups, s_name ):

        # Add column of heat map values to block groups dataframe
        for idx, row in df_block_groups.copy().iterrows():

            # Find residential parcels in current block group
            df_cbg_res = df_res_parcels[df_res_parcels[TRACT_DASH_GROUP] == row[TRACT_DASH_GROUP]]

            # Find which residential parcels are not weatherized
            df_cbg_nwx = df_cbg_res[ df_cbg_res[WX_PERMIT].isnull() ]

            # Count households that are not weatherized
            n_nwx = df_cbg_nwx[OCC].sum()
            df_block_groups.at[idx, HEAT_MAP_VALUE] = n_nwx

        df_block_groups[HEAT_MAP_VALUE] = df_block_groups[HEAT_MAP_VALUE].astype( int )
        df_block_groups[s_name] = df_block_groups[HEAT_MAP_VALUE]

        return df_block_groups


    def households_wx( df_res_parcels, df_stats, df_block_groups, s_name ):

        # Add column of heat map values to block groups dataframe
        for idx, row in df_block_groups.copy().iterrows():

            # Find residential parcels in current block group
            df_cbg_res = df_res_parcels[df_res_parcels[TRACT_DASH_GROUP] == row[TRACT_DASH_GROUP]]

            # Find which residential parcels are weatherized
            df_cbg_wx = df_cbg_res[ ~df_cbg_res[WX_PERMIT].isnull() ]

            # Count households that are weatherized
            n_wx = df_cbg_wx[OCC].sum()
            df_block_groups.at[idx, HEAT_MAP_VALUE] = n_wx

        df_block_groups[HEAT_MAP_VALUE] = df_block_groups[HEAT_MAP_VALUE].astype( int )
        df_block_groups[s_name] = df_block_groups[HEAT_MAP_VALUE]

        return df_block_groups


    def households_poverty( df_res_parcels, df_stats, df_block_groups, s_name ):
        return make_heat_map_from_stats( df_stats, df_block_groups, s_name )


    def households_poverty_pct( df_res_parcels, df_stats, df_block_groups, s_name ):
        return make_heat_map_from_stats( df_stats, df_block_groups, s_name )


    def households_wx_pct( df_res_parcels, df_stats, df_block_groups, s_name ):

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

    def population( df_res_parcels, df_stats, df_block_groups, s_name ):
        return make_heat_map_from_stats( df_stats, df_block_groups, s_name )


    def health_risk_score( df_res_parcels, df_stats, df_block_groups, s_name ):
        return make_heat_map_from_stats( df_stats, df_block_groups, s_name )


    def population_poverty( df_res_parcels, df_stats, df_block_groups, s_name ):
        return make_heat_map_from_stats( df_stats, df_block_groups, s_name )


    def res_parcels_nwx( df_res_parcels, df_stats, df_block_groups, s_name ):

        # Add column of heat map values to block groups dataframe
        for idx, row in df_block_groups.copy().iterrows():

            # Find residential parcels in current block group
            df_cbg_res = df_res_parcels[df_res_parcels[TRACT_DASH_GROUP] == row[TRACT_DASH_GROUP]]

            # Find which residential parcels are weatherized
            df_cbg_nwx = df_cbg_res[ df_cbg_res[WX_PERMIT].isnull() ]

            # Count weatherized residential parcels for current block group
            n_nwx = len( df_cbg_nwx )
            df_block_groups.at[idx, HEAT_MAP_VALUE] = n_nwx

        df_block_groups[HEAT_MAP_VALUE] = df_block_groups[HEAT_MAP_VALUE].astype( int )
        df_block_groups[s_name] = df_block_groups[HEAT_MAP_VALUE]

        return df_block_groups


    def res_parcels_wx( df_res_parcels, df_stats, df_block_groups, s_name ):

        # Add column of heat map values to block groups dataframe
        for idx, row in df_block_groups.copy().iterrows():

            # Find residential parcels in current block group
            df_cbg_res = df_res_parcels[df_res_parcels[TRACT_DASH_GROUP] == row[TRACT_DASH_GROUP]]

            # Find which residential parcels are weatherized
            df_cbg_wx = df_cbg_res[ ~df_cbg_res[WX_PERMIT].isnull() ]

            # Count weatherized residential parcels for current block group
            n_wx = len( df_cbg_wx )
            df_block_groups.at[idx, HEAT_MAP_VALUE] = n_wx

        df_block_groups[HEAT_MAP_VALUE] = df_block_groups[HEAT_MAP_VALUE].astype( int )
        df_block_groups[s_name] = df_block_groups[HEAT_MAP_VALUE]

        return df_block_groups


    def res_parcels_wx_pct( df_res_parcels, df_stats, df_block_groups, s_name ):

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


def make_heat_map_from_stats( df_stats, df_block_groups, s_name ):

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


# Calculate health risk score for each census block group
def get_health_risk_score( engine ):

    # Read the EJ screen summary table
    s_table = 'EJScreenSummary_L'
    print( f'Reading {s_table}' )
    print( '' )
    df_score = pd.read_sql_table( s_table, engine, columns=[util.CENSUS_GEO_ID, util.HEALTH_RISK_SCORE] )
    df_score = df_score.rename( columns={ util.CENSUS_GEO_ID: GEOID } )

    return df_score


# Prepare dataframe of block group statistics for use in heat map generation
def get_block_group_stats( census_data_filename, df_score ):

    # Read census statistics
    df_stats = pd.read_csv( census_data_filename )

    # Merge in health risk score
    df_stats = pd.merge( df_stats, df_score, how='left', on=[GEOID] )

    # Rename columns
    dc_rename = \
    {
        'median_household_income': HOUSEHOLDS_MEDIAN_INCOME,
        'poverty_population': POPULATION_POVERTY,
        'poverty_households': HOUSEHOLDS_POVERTY,
    }
    df_stats = df_stats.rename( columns=dc_rename )

    # Calculate poverty percentage
    df_stats[HOUSEHOLDS_POVERTY_PCT] = round( 100 * df_stats[HOUSEHOLDS_POVERTY] / df_stats[HOUSEHOLDS] )
    df_stats[HOUSEHOLDS_POVERTY_PCT] = df_stats[HOUSEHOLDS_POVERTY_PCT].astype( int )

    # Fix datatype
    df_stats[GEOID] = df_stats[GEOID].astype(str)

    return df_stats


# Prepare dataframe of residential parcels for use in heat map generation
def get_res_parcels( engine ):

    # Read the parcels table
    print( '' )
    s_table = 'Assessment_L_Parcels_Merged'
    print( f'Reading {s_table}' )
    df_parcels = pd.read_sql_table( s_table, engine )

    # Prepare dataframe of residential parcels
    df_res_parcels = df_parcels.copy()
    df_res_parcels = df_res_parcels[df_res_parcels[util.IS_RESIDENTIAL] == util.YES]

    # Add census block group labeling
    df_res_parcels[TRACT_DASH_GROUP] = df_res_parcels[util.CENSUS_TRACT].astype(str) + '-' + df_res_parcels[util.CENSUS_BLOCK_GROUP].astype(str)

    return df_res_parcels


# Generate heat map styles
def make_heat_map_styles( df_block_groups, kml, dc_heat_map_attrs ):

    s_label = dc_heat_map_attrs[HEAT_MAP_LABEL]

    # Add spectrum index column to block groups dataframe
    f_min_rate = df_block_groups[HEAT_MAP_VALUE].min()
    f_max_rate = df_block_groups[HEAT_MAP_VALUE].max()
    f_normalized_rate = f_max_rate - f_min_rate

    for idx, row in df_block_groups.copy().iterrows():
        df_block_groups.at[idx, SPECTRUM_INDEX] = ( len( HEAT_MAP_SPECTRUM ) - 1 ) * ( row[HEAT_MAP_VALUE] - f_min_rate ) / f_normalized_rate
    df_block_groups[SPECTRUM_INDEX] = df_block_groups[SPECTRUM_INDEX].astype( int )

    # Populate dictionary of block group styles
    dc_heat_map_styles = {}
    doc = kml.newdocument( name=s_label )
    doc.visibility = dc_heat_map_attrs[HEAT_MAP_VISIBILITY]

    for idx, row in df_block_groups.iterrows():
        cbg_style = simplekml.Style()
        cbg_style.linestyle.color = simplekml.Color.white
        cbg_style.linestyle.width = 4
        cbg_style.polystyle.fill = 1
        rgb = HEAT_MAP_SPECTRUM[row[SPECTRUM_INDEX]]
        cbg_style.polystyle.color = simplekml.Color.changealphaint( 150, simplekml.Color.rgb( *rgb ))

        # Save style in document and dictionary
        doc.styles.append( cbg_style )
        dc_heat_map_styles[row[TRACT_DASH_GROUP]] = cbg_style

    return doc, dc_heat_map_styles


# Generate KML heat map of data partitioned by census block groups
def make_heat_map_kml_file( kml, doc, df_block_groups, dc_heat_map_attrs, dc_heat_map_styles, output_directory, s_name ):

    s_prefix = dc_heat_map_attrs[HEAT_MAP_PREFIX]
    s_unit = dc_heat_map_attrs[HEAT_MAP_UNIT]
    s_label = dc_heat_map_attrs[HEAT_MAP_LABEL]

    # Enhance document name
    n_min = df_block_groups[HEAT_MAP_VALUE].min()
    n_max = df_block_groups[HEAT_MAP_VALUE].max()
    s_min = f'{s_prefix}{n_min:,}{s_unit}'
    s_max = f'{s_prefix}{n_max:,}{s_unit}'
    s_to = ' to '
    s_range = f': Range {s_min}{s_to}{s_max}'
    doc.name += s_range

    # Generate polygon for each census block group
    for idx, row in df_block_groups.iterrows():
        s_block_group = row[TRACT_DASH_GROUP]
        n_value = row[HEAT_MAP_VALUE]
        s_value = f'{s_prefix}{n_value:,}{s_unit}'
        poly = doc.newpolygon( name=f'CBG {s_block_group}: {s_value}' )
        poly.outerboundaryis = list( row[util.GEOMETRY].exterior.coords )
        poly.description = f'<p>Geographic ID: {row[GEOID]}</p><p>{s_label}: {s_value}</p>'
        poly.style = dc_heat_map_styles[s_block_group]

    # Save the KML file
    print( f'Saving heat map "{s_label}"' )
    kml.save( os.path.join( output_directory, 'maps', f'{s_name}.kml' ) )

    return kml


# Replace simplekml-generated integer IDs with unique IDs
def replace_ids_with_uuids( kml, output_directory, s_name ):

    # Get XML string from Kml object
    s_kml = kml.kml()

    # Parse XML
    root = ET.fromstring( s_kml )

    # Set the KML namespace
    ET.register_namespace( '', KML_NAMESPACE )

    # Replace simplekml ID with UUID and save mappings in dictionary
    dc_id_uuid = {}
    for elem in root.iter():
        elem_id = elem.get( 'id' )
        if elem_id:
            new_id = str( uuid.uuid4() )
            dc_id_uuid[elem_id] = new_id
            elem.set( 'id', new_id )

    # Replace references to old IDs
    for elem in root.iter():
        if elem.text and elem.text.startswith( '#' ):
            ref_id = elem.text[1:]
            if ref_id in dc_id_uuid:
                elem.text = '#' + dc_id_uuid[ref_id]

    # Return revised KML
    xml = ET.ElementTree( root )

    return xml


# Combine heat maps under a parent folder
def combine_heat_maps( s_folder, output_directory=None ):

    namespace = KML_NAMESPACE
    ET.register_namespace( '', namespace )

    # Create new KML root
    schema = f'{{{namespace}}}'
    root = ET.Element( f'{schema}kml' )
    doc = ET.SubElement( root, f'{schema}Document' )

    # Get folder attributes
    dc_folder = DC_FOLDERS[s_folder]

    # Create parent folder that will contain all heat maps
    parent_folder = ET.SubElement( doc, f'{schema}Folder' )
    ET.SubElement( parent_folder, f'{schema}name' ).text = dc_folder[FOLDER_LABEL]

    # Iterate through saved heat maps
    for s_heat_map in dc_folder[FOLDER_CONTENTS]:

        # Start with root of saved heat map
        kml = DC_HEAT_MAPS[s_heat_map][KML]

        # -> Commented out -> not needed ->
        #
        # Replace simplekml-generated integer IDs with unique IDs and save as tree
        # xml = replace_ids_with_uuids( kml, args.output_directory, s_name )
        #
        # <- Commented out <- not needed <-
        #
        # -> Instead, do this ->
        #
        xml = ET.ElementTree( ET.fromstring( kml.kml() ) )
        #
        # <- Instead, do this <-

        xml_root = xml.getroot()

        # Find <Document> element of the source
        src_doc = xml_root.find( f'{schema}Document' )

        # Append all children of the source KML to the parent folder
        for child in list( src_doc ):
            parent_folder.append( util.replicate_kml_element( child ) )

    # Save folder of combined heat maps
    xml = ET.ElementTree( root )
    dc_folder[XML] = xml

    # Optionally write the result to a KML file
    if output_directory:
        print( f'Saving folder "{dc_folder[FOLDER_LABEL]}"' )
        output_path = os.path.join( output_directory, 'folders', f'{s_folder}.kml' )
        xml.write( output_path, encoding='utf-8', xml_declaration=True )


# Build full heat maps tree for Google Earth presentation
def make_heat_maps_tree( output_directory ):

    # Build Demographics tree
    ls_children = [DC_FOLDERS[POPULATION_FOLDER][XML], DC_FOLDERS[HOUSEHOLDS_FOLDER][XML], DC_FOLDERS[HEALTH_FOLDER][XML]]
    output_path = os.path.join( output_directory, 'trees', 'demographics_tree.kml' )
    demo_tree = util.insert_in_parent_kml_folder( ls_children, 'Demographics', output_path=output_path )

    # Build Heating Fuel tree
    ls_children = [DC_FOLDERS[HEATING_FUEL_FOLDER][XML]]
    output_path = os.path.join( output_directory, 'trees', 'heating_fuel_tree.kml' )
    fuel_tree = util.insert_in_parent_kml_folder( ls_children, 'Heating Fuel', output_path=output_path )

    # Build Weatherization tree
    ls_children = [DC_FOLDERS[WX_HOUSEHOLDS_FOLDER][XML], DC_FOLDERS[WX_PARCELS_FOLDER][XML]]
    output_path = os.path.join( output_directory, 'trees', 'weatherization_tree.kml' )
    wx_tree = util.insert_in_parent_kml_folder( ls_children, 'Weatherization', output_path=output_path )

    # Build full Heat Maps tree
    ls_children = [demo_tree, fuel_tree, wx_tree]
    output_path = os.path.join( output_directory, 'heat_maps.kml' )
    util.insert_in_parent_kml_folder( ls_children, 'Heat Maps', output_path=output_path )



##################################


# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate KML heat maps showing various metrics pertaining to Lawrence census block groups' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename', required=True )
    parser.add_argument( '-b', dest='block_groups_filename',  help='Input filename - Name of shapefile containing Lawrence block group geometry', required=True )
    parser.add_argument( '-c', dest='census_data_filename',  help='Input filename - Name of CSV file containing US Census data partitioned by block groups', required=True )
    parser.add_argument( '-o', dest='output_directory', help='Target directory for output files', required=True )

    args = parser.parse_args()

    # Report arguments
    print( '' )
    print( 'Arguments' )
    print( ' Input database:', args.master_filename )
    print( ' Block Groups filename:', args.block_groups_filename )
    print( ' Census Data filename:', args.census_data_filename )
    print( ' Output directory:', args.output_directory )

    # Open the database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Extract dataframe of residential parcels for generating the heat map
    df_res_parcels = get_res_parcels( engine )

    # Calculate health risk score
    df_score = get_health_risk_score( engine )

    # Get dataframe of block group statistics
    df_stats = get_block_group_stats( args.census_data_filename, df_score )

    # Extract block group geometries from shapefile
    df_block_groups = util.get_block_groups_geometry( args.block_groups_filename )
    df_block_groups[HEAT_MAP_VALUE] = None

    # Generate heat maps
    for s_name in DC_HEAT_MAPS:

        # Get attributes of current heat map
        dc_heat_map_attrs = DC_HEAT_MAPS[s_name]

        # Compute heat map values
        f = getattr( Compute, s_name )
        df_block_groups = f( df_res_parcels, df_stats, df_block_groups, s_name )

        # Create empty KML
        kml = simplekml.Kml()

        # Generate heat map styles
        doc, dc_heat_map_styles = make_heat_map_styles( df_block_groups, kml, dc_heat_map_attrs )

        # Generate KML heat map file
        DC_HEAT_MAPS[s_name][KML] = make_heat_map_kml_file( kml, doc, df_block_groups, dc_heat_map_attrs, dc_heat_map_styles, args.output_directory, s_name )


    # Group heat maps into KML folders
    print( '' )
    for s_folder in DC_FOLDERS:
        combine_heat_maps( s_folder, output_directory=args.output_directory )

    # Group folders into tree structure
    print( '' )
    make_heat_maps_tree( args.output_directory )

    # Save heat map values to database
    df_block_groups = df_block_groups.drop( columns=[util.GEOMETRY, HEAT_MAP_VALUE, SPECTRUM_INDEX] )
    util.create_table( 'HeatMaps_L', conn, cur, df=df_block_groups )

    util.report_elapsed_time()
