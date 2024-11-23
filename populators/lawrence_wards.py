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
    util.PRECINCT_NUMBER,
    util.TOTAL_OCCUPANCY,
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
    util.SOLAR_PERMIT,
    util.WX_PERMIT,
    util.TOTAL_ACRES,
    util.SALE_PRICE,
    util.SALE_DATE,
    util.ZONE,
    util.OWNER_1_NAME,
    util.OWNER_2_NAME,
    util.OWNER_ADDRESS,
    util.OWNER_ZIP,
    util.ZIP,
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
    '01840': util.ZIP + '_01840',
    '01841': util.ZIP + '_01841',
    '01842': util.ZIP + '_01842',
    '01843': util.ZIP + '_01843',
}

RENTAL_MAP_2_4 = \
{
    2: util.TOTAL_OCCUPANCY + '_2',
    3: util.TOTAL_OCCUPANCY + '_3',
    4: util.TOTAL_OCCUPANCY + '_4',
}

RENTAL_MAP_5_7 = \
{
    5: util.TOTAL_OCCUPANCY + '_5',
    6: util.TOTAL_OCCUPANCY + '_6',
    7: util.TOTAL_OCCUPANCY + '_7',
}


def build_summary_table( df_summary, df_details ):
    # Add columns counting per-ward occurrences of specified heating fuels
    df_summary = util.add_value_counts( df_summary, df_details, util.WARD_NUMBER, util.HEATING_FUEL_DESC, HEATING_FUEL_MAP )

    # Add columns counting per-ward occurrences of specified heating types
    df_summary = util.add_value_counts( df_summary, df_details, util.WARD_NUMBER, util.HEATING_TYPE_DESC, HEATING_TYPE_MAP )

    # Add columns counting per-ward occurrences of specified zip codes
    df_summary = util.add_value_counts( df_summary, df_details, util.WARD_NUMBER, util.ZIP, ZIP_CODE_MAP )

    return df_summary


# Create one parcels table for each ward
def create_wards_parcels_tables( df_parcels ):

    # Group parcels by wards
    for ward, df_group in df_parcels.groupby( by=[util.WARD_NUMBER] ):

        # Create, sort, and save parcels table for current ward
        df_ward_parcels = df_group[WARD_PARCELS_COLUMNS]
        df_ward_parcels = df_ward_parcels.sort_values( by=[util.PRECINCT_NUMBER, util.TOTAL_OCCUPANCY, util.VISION_ID] )
        util.create_table( 'Ward_{}_ResidentialParcels'.format( ward ), conn, cur, df=df_ward_parcels )

    return


# Create wards summary table
def create_wards_summary_table( df_councilors, df_parcels ):

    # Build and save wards summary table
    df_summary = df_councilors.copy()
    df_summary = build_summary_table( df_summary, df_parcels )
    util.create_table( 'WardSummary', conn, cur, df=df_summary )

    return


# Create wards summary of small rentals
def create_wards_summary_table_small( df_councilors, df_parcels ):

    # Isolate small rental properties
    df_rentals_small = df_parcels[( df_parcels[util.TOTAL_OCCUPANCY] >= 2 ) & ( df_parcels[util.TOTAL_OCCUPANCY] <= 4 )]

    # Initialize small rental summary
    df_summary = df_councilors.copy()

    # Count parcels with specific total occupancies
    df_summary = util.add_value_counts( df_summary, df_rentals_small, util.WARD_NUMBER, util.TOTAL_OCCUPANCY, RENTAL_MAP_2_4 )

    # Count total small-rental parcels
    df_summary[util.TOTAL_OCCUPANCY + '_2_4'] = df_summary[util.TOTAL_OCCUPANCY + '_2'] + df_summary[util.TOTAL_OCCUPANCY + '_3'] + df_summary[util.TOTAL_OCCUPANCY + '_4']

    # Count total small-rental residential units
    for ward, df_group in df_rentals_small.groupby( by=[util.WARD_NUMBER] ):
        summary_row_index = df_summary.loc[ df_summary[util.WARD_NUMBER] == ward ].index
        df_summary.at[summary_row_index, util.TOTAL_RESIDENTIAL_UNITS] = df_group[util.TOTAL_OCCUPANCY].sum()
    df_summary[util.TOTAL_RESIDENTIAL_UNITS] = df_summary[util.TOTAL_RESIDENTIAL_UNITS].astype(int)

    # Build the rest of the small rental summary
    df_summary = build_summary_table( df_summary, df_rentals_small )

    # Save small rental summary
    util.create_table( 'WardSummary_Rentals_2_4', conn, cur, df=df_summary )

    return


# Create wards summary of large rentals
def create_wards_summary_table_large( df_councilors, df_parcels ):

    # Isolate large rental properties
    df_rentals_large = df_parcels[( df_parcels[util.TOTAL_OCCUPANCY] > 4 ) ]

    # Initialize large rental summary
    df_summary = df_councilors.copy()

    # Count parcels with specific total occupancies
    df_summary = util.add_value_counts( df_summary, df_rentals_large, util.WARD_NUMBER, util.TOTAL_OCCUPANCY, RENTAL_MAP_5_7 )

    # Count total large-rental parcels
    for ward, df_group in df_rentals_large.groupby( by=[util.WARD_NUMBER] ):
        summary_row_index = df_summary.loc[ df_summary[util.WARD_NUMBER] == ward ].index
        df_summary.at[summary_row_index, util.TOTAL_OCCUPANCY + '_gt_4'] = len( df_group )
    df_summary[util.TOTAL_OCCUPANCY + '_gt_4'] = df_summary[util.TOTAL_OCCUPANCY + '_gt_4'].astype(int)

    # Count parcels with greatest total occupancies
    df_summary[util.TOTAL_OCCUPANCY + '_8_or_more'] = df_summary[util.TOTAL_OCCUPANCY + '_gt_4'] - ( df_summary[util.TOTAL_OCCUPANCY + '_5'] + df_summary[util.TOTAL_OCCUPANCY + '_6'] + df_summary[util.TOTAL_OCCUPANCY + '_7'] )

    # Swap positions of last two columns
    cols = df_summary.columns.tolist()
    cols[-1], cols[-2] = cols[-2], cols[-1]
    df_summary = df_summary[cols]

    # Count total large-rental residential units
    for ward, df_group in df_rentals_large.groupby( by=[util.WARD_NUMBER] ):
        summary_row_index = df_summary.loc[ df_summary[util.WARD_NUMBER] == ward ].index
        df_summary.at[summary_row_index, util.TOTAL_RESIDENTIAL_UNITS] = df_group[util.TOTAL_OCCUPANCY].sum()
    df_summary[util.TOTAL_RESIDENTIAL_UNITS] = df_summary[util.TOTAL_RESIDENTIAL_UNITS].astype(int)

    # Build the rest of the large rental summary
    df_summary = build_summary_table( df_summary, df_rentals_large )

    # Save large rental summary
    util.create_table( 'WardSummary_Rentals_Gt4', conn, cur, df=df_summary )
    
    return


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

    # Create collections of parcels tables, one for each ward
    create_wards_parcels_tables( df_parcels )

    # Load councilors dataframe
    df_councilors = pd.read_sql_table( 'Councilors_L', engine, index_col=util.ID, parse_dates=True )

    # Create wards summary table
    create_wards_summary_table( df_councilors, df_parcels )

    # Create wards summary of small rentals
    create_wards_summary_table_small( df_councilors, df_parcels )

    # Create wards summary of large rentals
    create_wards_summary_table_large( df_councilors, df_parcels )

    util.report_elapsed_time()
