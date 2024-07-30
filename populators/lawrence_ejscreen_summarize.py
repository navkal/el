# Copyright 2024 Energize Andover.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append('../util')
import util


# Calculate number of tenant households per census block group
def calculate_tenant_households( df_ej ):

    # Determine how many owner households there are in each census block group
    df_ej[util.OWNER_HOUSEHOLDS] = ( df_ej[util.PCT_OWNER_OCCUPIED] * df_ej[util.PARCEL_COUNT] / 100 ).round( decimals=0 )

    # Subtract owner households from total households to get tenant households
    df_ej[util.TENANT_HOUSEHOLDS] = df_ej[util.TOTAL_HOUSEHOLDS] - df_ej[util.OWNER_HOUSEHOLDS]

    return df_ej


# Add column counting number of parcels per census block group
def add_parcel_counts( df_ej, df_parcels ):

    # Initialize count column
    df_ej[util.PARCEL_COUNT] = 0

    # Iterate over parcels grouped by census block groups
    for census_geo_id, df_group in df_parcels.groupby( by=[util.CENSUS_GEO_ID] ):

        # Find corresponding summary row
        ej_row_index = df_ej.loc[df_ej[util.CENSUS_GEO_ID] == census_geo_id].index[0]

        # Save the count in the summary table
        df_ej.at[ej_row_index, util.PARCEL_COUNT] = len( df_group )

    return df_ej


# Add column to count per-block-group occurrences of specified values in a specified column
def add_value_counts( df_ej, df_parcels, s_parcels_col, value_map ):

    # Initialize count columns
    for s_key in value_map:
        df_ej[value_map[s_key]] = 0

    # Iterate over parcels grouped by census block groups
    for census_geo_id, df_group in df_parcels.groupby( by=[util.CENSUS_GEO_ID] ):

        # Find corresponding summary row
        ej_row_index = df_ej.loc[df_ej[util.CENSUS_GEO_ID] == census_geo_id].index[0]

        # Save the counts in the summary table
        for s_key in value_map:
            df_ej.at[ej_row_index, value_map[s_key]] = len( df_group[df_group[s_parcels_col] == s_key] )

    return df_ej


# Add column to summary table containing per-block-group sums from specified parcels table column
def add_parcels_sum_column( df_ej, df_parcels, s_parcels_col ):

    # Initialize sum column
    df_ej[s_parcels_col] = 0

    # Iterate over parcels grouped by census block groups
    for census_geo_id, df_group in df_parcels.groupby( by=[util.CENSUS_GEO_ID] ):

        # Find corresponding summary row
        ej_row_index = df_ej.loc[df_ej[util.CENSUS_GEO_ID] == census_geo_id].index[0]

        # Save the sum in summary table
        df_ej.at[ej_row_index, s_parcels_col] = df_group[s_parcels_col].sum()

    return df_ej


# Add column to EJScreen summary table counting permits of specified type
def add_permit_counts( df_ej, df_parcels, s_permit_type ):

    # Initialize column names for current permit type
    permit_col_name = s_permit_type + '_permit'
    count_col_name = permit_col_name + '_count'

    # Initialize count column
    df_ej[count_col_name] = 0

    # Iterate over parcels grouped by census block groups (identified by census geo id)
    for census_geo_id, df_group in df_parcels.groupby( by=[util.CENSUS_GEO_ID] ):

        # Isolate parcels in current block group that have at least one permit of specified permit type
        df_parcels_with_permits = df_group[ df_group[permit_col_name].notna() ]

        # Count the permits in current block group
        n_permits = 0
        for i_row, row in df_parcels_with_permits.iterrows():
            n_permits += ( row[permit_col_name].count(',') + 1 )

        # Save the permit count in the corresponding row of the EJScreen summary table
        ej_row_index = df_ej.loc[df_ej[util.CENSUS_GEO_ID] == census_geo_id].index[0]
        df_ej.at[ej_row_index, count_col_name] = n_permits

    return df_ej


MV_COLUMNS= \
[
    util.CENSUS_GEO_ID,
    util.TOTAL_MV,
    util.HEAVY_DUTY_MV,
    util.MEDIUM_DUTY_MV,
    util.LIGHT_DUTY_MV,
    util.FOSSIL_FUEL_MV,
    util.HYBRID_MV,
    util.ZERO_EMISSION_MV,
    util.EV,
    util.PHEV,
]


# Add columns to EJScreen summary table counting vehicle types
def add_vehicle_counts( df_ej, df_mv ):

    # Initialize table summarizing motor vehicle counts
    df_mv_summary = pd.DataFrame( columns=MV_COLUMNS )

    for census_geo_id, df_group in df_mv.groupby( by=[util.CENSUS_GEO_ID] ):

        # Initialize empty summary row
        mv_summary_row = dict( ( el, 0 ) for el in MV_COLUMNS )

        # Load summary row
        mv_summary_row[util.CENSUS_GEO_ID] = census_geo_id
        mv_summary_row[util.TOTAL_MV] = df_group[util.COUNT].sum()

        col = util.GVWR_CATEGORY
        mv_summary_row[util.HEAVY_DUTY_MV] = df_group[ df_group[col] == 'Heavy Duty'][util.COUNT].sum()
        mv_summary_row[util.MEDIUM_DUTY_MV] = df_group[ df_group[col] == 'Medium Duty'][util.COUNT].sum()
        mv_summary_row[util.LIGHT_DUTY_MV] = df_group[ df_group[col] == 'Light Duty'][util.COUNT].sum()

        col = util.FUEL_CLASS
        mv_summary_row[util.FOSSIL_FUEL_MV] = df_group[ df_group[col] == 'Fossil Fuel'][util.COUNT].sum()
        mv_summary_row[util.ZERO_EMISSION_MV] = df_group[ df_group[col] == 'Zero-Emission'][util.COUNT].sum()
        mv_summary_row[util.HYBRID_MV] = df_group[ df_group[col] == 'Hybrid'][util.COUNT].sum()

        col = util.ADVANCED_VEHICLE_TYPE
        mv_summary_row[util.EV] = df_group[ df_group[col] == 'Electric Vehicle'][util.COUNT].sum()
        mv_summary_row[util.PHEV] = df_group[ df_group[col] == 'Plug-in Hybrid Electric Vehicle'][util.COUNT].sum()

        df_mv_summary = df_mv_summary.append( mv_summary_row, ignore_index=True )

    # Ensure numeric datatype
    df_mv_summary = util.fix_numeric_columns( df_mv_summary )

    # Merge motor vehicle summary to EJScreen summary ('outer' to capture all possible geo IDs)
    df_ej = pd.merge( df_ej, df_mv_summary, how='outer', on=[util.CENSUS_GEO_ID] )

    # Now restore integer-type columns that were messed up by the merge
    df_ej = util.float_to_int( df_ej )

    return df_ej


######################

# Main program
if __name__ == '__main__':

    # Retrieve arguments
    parser = argparse.ArgumentParser( description='Summarize Lawrence EJScreen data' )
    parser.add_argument( '-e', dest='ejscreen_filename',  help='EJScreen database filename' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open EJScreen database
    conn, cur, engine = util.open_database( args.ejscreen_filename, False )

    # Read full EJScreen table from database
    df_ej = pd.read_sql_table( 'EJScreen_L', engine, index_col=util.ID, parse_dates=True )

    # Get list of columns to drop from EJScreen table
    df_drop = pd.read_sql_table( 'StatePercentilesDataset', engine, index_col=util.ID, parse_dates=True )
    ls_drop = list( df_drop[ df_drop['dropped'] == util.YES ][util.GDB_FIELDNAME] )

    # Drop columns from table
    df_ej = df_ej.drop( columns=ls_drop )

    # Open master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Read table of energy meter participation rates and merge into EJScreen table
    df_mp = pd.read_sql_table( 'RawEnergyMeterParticipation_L', engine, index_col=util.ID, parse_dates=True )
    df_ej = pd.merge( df_ej, df_mp, how='outer', on=[util.CENSUS_GEO_ID] )

    # Read parcels table from database and select residential parcels with known block groups
    df_parcels = pd.read_sql_table( 'Assessment_L_Parcels', engine, index_col=util.ID, parse_dates=True )
    df_parcels = df_parcels[( df_parcels[util.IS_RESIDENTIAL] == util.YES ) & ( df_parcels[util.CENSUS_GEO_ID] != 0 )]

    # Add columns counting per-block-group occurrences of specified heating fuels
    HEATING_FUEL_MAP = \
    {
        'Electric': 'heating_fuel_electric',
        'Gas': 'heating_fuel_gas',
        'Oil': 'heating_fuel_oil',
    }
    df_ej = add_value_counts( df_ej, df_parcels, util.HEATING_FUEL_DESC, HEATING_FUEL_MAP )

    # Add columns counting per-block-group occurrences of specified heating types
    HEATING_TYPE_MAP = \
    {
        'Steam': 'heating_type_steam',
        'Radiant': 'heating_type_radiant',
        'None': 'heating_type_none',
        'Hot Water': 'heating_type_hot_water',
        'Hot Air-no Duc': 'heating_type_hot_air_no_duc',
        'Heat Pump': 'heating_type_heat_pump',
        'Forced Air-Duc': 'heating_type_forced_air_duc',
        'Floor Furnace': 'heating_type_floor_furnace',
        'Electr Basebrd': 'heating_type_electr_basebrd',
    }
    df_ej = add_value_counts( df_ej, df_parcels, util.HEATING_TYPE_DESC, HEATING_TYPE_MAP )

    # Add columns containing per-block-group sums of parcels table columns
    PARCELS_COLUMNS= \
    [
        util.TOTAL_OCCUPANCY,
    ]
    for s_parcels_col in PARCELS_COLUMNS:
        df_ej = add_parcels_sum_column( df_ej, df_parcels, s_parcels_col )

    # Add column counting number of parcels per census block group
    df_ej = add_parcel_counts( df_ej, df_parcels )

    # Read table of owner-occupied percentages and merge into EJScreen table
    df_oo = pd.read_sql_table( 'RawOwnerOccupied_L', engine, index_col=util.ID, parse_dates=True )
    df_ej = pd.merge( df_ej, df_oo, how='left', on=[util.CENSUS_GEO_ID] )

    # Calculate the number of tenant households per census block group
    df_ej = calculate_tenant_households( df_ej )

    # Add columns containing per-block-group permit counts
    for s_permit_type in util.BUILDING_PERMIT_TYPES:
        df_ej = add_permit_counts( df_ej, df_parcels, s_permit_type )

    # Add columns containing per-block-group vehicle counts
    df_mv = pd.read_sql_table( 'MotorVehicles_L', engine, index_col=util.ID, parse_dates=True )
    df_ej = add_vehicle_counts( df_ej, df_mv )

    # Fix datatypes
    df_ej[util.PCT_OWNER_OCCUPIED] = df_ej[util.PCT_OWNER_OCCUPIED].fillna( 0 ).astype( int )
    df_ej[util.OWNER_HOUSEHOLDS] = df_ej[util.OWNER_HOUSEHOLDS].fillna( 0 ).astype( int )
    for s_key in HEATING_FUEL_MAP:
        df_ej[HEATING_FUEL_MAP[s_key]] = df_ej[HEATING_FUEL_MAP[s_key]].fillna( 0 ).astype( int )
    for s_key in HEATING_TYPE_MAP:
        df_ej[HEATING_TYPE_MAP[s_key]] = df_ej[HEATING_TYPE_MAP[s_key]].fillna( 0 ).astype( int )

    # Sort on census global ID
    df_ej = df_ej.sort_values( by=[util.CENSUS_GEO_ID] )

    # Save summary table to master database
    util.create_table( 'EJScreenSummary_L', conn, cur, df=df_ej )

    # Report elapsed time
    util.report_elapsed_time()
