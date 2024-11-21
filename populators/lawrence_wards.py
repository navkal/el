# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util




##################################

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Generate ward tables' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Read Councilors table
    df_councilors = pd.read_sql_table( 'Councilors_L', engine, index_col=util.ID, parse_dates=True )

    # Read Parcels table
    df_parcels = pd.read_sql_table( 'Assessment_L_Parcels', engine, index_col=util.ID, parse_dates=True )

    # Isolate residential properties with known wards
    df_parcels = df_parcels[( df_parcels[util.IS_RESIDENTIAL] == util.YES ) & ( df_parcels[util.WARD_NUMBER].notnull() )]

    # Initialize empty wards summary table
    summary_columns = \
    [
        util.COUNCILOR_NAME,
        util.WARD_NUMBER,
    ]
    df_summary = pd.DataFrame( columns=summary_columns )

    # Initialize empty dictionary of ward parcels tables
    ward_parcels_columns = \
    [
        util.VISION_ID,
        util.ACCOUNT_NUMBER,
        util.MBLU,
        util.IS_RESIDENTIAL,
        util.OWNER_IS_LOCAL,
        util.NORMALIZED_STREET_NAME,
        util.LOCATION,
        util.YEAR_BUILT,
        util.AGE,
        util.BUILDING_COUNT,
        util.TOTAL_ASSESSED_VALUE,
        util.HEATING_FUEL_DESC,
        util.HEATING_TYPE_DESC,
        util.AC_TYPE_DESC,
        util.HEAT_AC,
        util.LAND_USE_CODE,
        util.LAND_USE_CODE_DESC,
        util.TOTAL_OCCUPANCY,
        util.TOTAL_ACRES,
        util.SALE_PRICE,
        util.SALE_DATE,
        util.ZONE,
        util.OWNER_1_NAME,
        util.OWNER_2_NAME,
        util.OWNER_ADDRESS,
        util.OWNER_ZIP,
        util.ZIP,
        util.WARD_NUMBER,
        util.PRECINCT_NUMBER,
        util.CENSUS_GEO_ID,
        util.CENSUS_TRACT,
        util.CENSUS_BLOCK_GROUP,
        util.VISION_LINK,
    ]
    dc_ward_parcels_dfs = {}

    # Group parcels by wards
    for idx, df_group in df_parcels.groupby( by=[util.WARD_NUMBER] ):

        #
        # Ward parcels table
        #

        # Create parcels table for current ward
        df_ward_parcels = df_group[ward_parcels_columns]

        # Save parcels table for this ward in dictionary
        dc_ward_parcels_dfs[idx] = df_ward_parcels

        #
        # Ward summary table
        #

        # Initialize ward row from councilors table
        summary_row = df_councilors[ df_councilors[util.WARD_NUMBER] == df_group.iloc[0][util.WARD_NUMBER] ].copy()

        # Add other fields to the row
        summary_row['electric'] = len( df_group[ df_group[util.HEATING_FUEL_DESC] == 'Electric' ] )
        summary_row['oil'] = len( df_group[ df_group[util.HEATING_FUEL_DESC] == 'Oil' ] )
        summary_row['gas'] = len( df_group[ df_group[util.HEATING_FUEL_DESC] == 'Gas' ] )

        # Append new ward row to wards summary table
        df_summary = pd.concat( [df_summary, summary_row] )

    # Save ward tables in database
    for ward in dc_ward_parcels_dfs:
        util.create_table( 'Ward_' + ward, conn, cur, df=dc_ward_parcels_dfs[ward] )

    # Save summary table in database
    util.create_table( 'WardSummary', conn, cur, df=df_summary )

    util.report_elapsed_time()
