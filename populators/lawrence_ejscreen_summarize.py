# Copyright 2024 Energize Andover.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append('../util')
import util


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

    # Merge motor vehicle summary table to EJScreen summary
    df_ej = pd.merge( df_ej, df_mv_summary, how='left', on=[util.CENSUS_GEO_ID] )

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

    # Read parcels table from database and select rows with known block groups
    df_parcels = pd.read_sql_table( 'Assessment_L_Parcels', engine, index_col=util.ID, parse_dates=True )
    df_parcels = df_parcels[df_parcels[util.CENSUS_GEO_ID] != 0]

    # Add columns containing per-block-group permit counts
    for s_permit_type in util.BUILDING_PERMIT_TYPES:
        df_ej = add_permit_counts( df_ej, df_parcels, s_permit_type )

    # Add columns containing per-block-group vehicle counts
    df_mv = pd.read_sql_table( 'MotorVehicles_L', engine, index_col=util.ID, parse_dates=True )
    df_ej = add_vehicle_counts( df_ej, df_mv )


    # Save summary table to master database
    util.create_table( 'EJScreenSummary_L', conn, cur, df=df_ej )

    # Report elapsed time
    util.report_elapsed_time()
