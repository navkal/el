# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util


WARD_PARCELS_COLUMNS = \
[
    util.VISION_ID,
    util.ACCOUNT_NUMBER,
    util.MBLU,
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

WARD_SUMMARY_COLUMNS = \
[
    util.COUNCILOR_NAME,
    util.WARD_NUMBER,
]

HEATING_FUEL_MAP = util.HEATING_FUEL_MAP
HEATING_TYPE_MAP = util.HEATING_TYPE_MAP

ZIP_CODE_MAP = \
{
    '01840': 'zip_code_01840',
    '01841': 'zip_code_01841',
    '01842': 'zip_code_01842',
    '01843': 'zip_code_01843',
}


##################################

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Generate ward tables' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Initialize table of residential parcels with known wards
    df_parcels = pd.read_sql_table( 'Assessment_L_Parcels', engine, index_col=util.ID, parse_dates=True )
    df_parcels = df_parcels[( df_parcels[util.IS_RESIDENTIAL] == util.YES ) & ( df_parcels[util.WARD_NUMBER].notnull() )]

    # Group parcels by wards
    for ward, df_group in df_parcels.groupby( by=[util.WARD_NUMBER] ):

        # Create and save current ward parcels table
        df_ward_parcels = df_group[WARD_PARCELS_COLUMNS]
        util.create_table( 'Ward_{}_ResidentialParcels'.format( ward ), conn, cur, df=df_ward_parcels )


    # Initialize wards summary table from Councilors spreadsheet
    df_summary = pd.read_sql_table( 'Councilors_L', engine, index_col=util.ID, parse_dates=True )

    # Add columns counting per-ward occurrences of specified heating fuels
    df_summary = util.add_value_counts( df_summary, df_parcels, util.WARD_NUMBER, util.HEATING_FUEL_DESC, HEATING_FUEL_MAP )

    # Add columns counting per-ward occurrences of specified heating types
    df_summary = util.add_value_counts( df_summary, df_parcels, util.WARD_NUMBER, util.HEATING_TYPE_DESC, HEATING_TYPE_MAP )

    # Add columns counting per-ward occurrences of specified zip codes
    df_summary = util.add_value_counts( df_summary, df_parcels, util.WARD_NUMBER, util.ZIP, ZIP_CODE_MAP )

    # Save wards summary table in database
    util.create_table( 'WardSummary', conn, cur, df=df_summary )

    util.report_elapsed_time()
