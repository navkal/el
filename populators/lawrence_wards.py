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
    util.ROOF_PERMIT,
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
    util.NATIONAL_GRID_R1_ACCOUNT,
    util.NATIONAL_GRID_R2_ACCOUNT,
    util.LEAN_ELIGIBILITY,
]

HEATING_FUEL_MAP = util.HEATING_FUEL_MAP
HEATING_TYPE_MAP = util.HEATING_TYPE_MAP
HEATING_FUEL_OCCUPANCY_MAP = util.HEATING_FUEL_OCCUPANCY_MAP

ELECTRIC_PARCELS = HEATING_FUEL_MAP[util.ELECTRIC]
ELECTRIC_OCCUPANCY = HEATING_FUEL_OCCUPANCY_MAP[util.ELECTRIC]
OIL_PARCELS = HEATING_FUEL_MAP[util.OIL]
OIL_OCCUPANCY = HEATING_FUEL_OCCUPANCY_MAP[util.OIL]
ELEC_OIL_PARCELS = util.ELEC_OIL_PARCELS
ELEC_OIL_OCCUPANCY = util.ELEC_OIL_OCCUPANCY


ZIP_CODE_MAP = \
{
    util.LAWRENCE_ZIPS[0]: util.ZIP + '_' + util.LAWRENCE_ZIPS[0],
    util.LAWRENCE_ZIPS[1]: util.ZIP + '_' + util.LAWRENCE_ZIPS[1],
    util.LAWRENCE_ZIPS[2]: util.ZIP + '_' + util.LAWRENCE_ZIPS[2],
    util.LAWRENCE_ZIPS[3]: util.ZIP + '_' + util.LAWRENCE_ZIPS[3],
}

RENTAL_MAP_2_4 = \
{
    2: util.PARCEL_COUNT + util.with_units( 2 ),
    3: util.PARCEL_COUNT + util.with_units( 3 ),
    4: util.PARCEL_COUNT + util.with_units( 4 ),
}

RENTAL_MAP_5_7 = \
{
    5: util.PARCEL_COUNT + util.with_units( 5 ),
    6: util.PARCEL_COUNT + util.with_units( 6 ),
    7: util.PARCEL_COUNT + util.with_units( 7 ),
}

BUILDING_PERMIT_TYPES = \
[
    util.ROOF,
    util.SOLAR,
    util.WX,
]


def add_common_summary_columns( df_summary, df_details, parcel_count_suffix='' ):

    # Add columns counting per-ward occurrences of specified heating fuels
    df_summary = util.add_value_counts( df_summary, df_details, util.WARD_NUMBER, util.HEATING_FUEL_DESC, HEATING_FUEL_MAP )

    # Add columns counting per-ward occurrences of specified heating types
    df_summary = util.add_value_counts( df_summary, df_details, util.WARD_NUMBER, util.HEATING_TYPE_DESC, HEATING_TYPE_MAP )

    # Add columns counting per-ward occurrences of specified zip codes
    df_summary = util.add_value_counts( df_summary, df_details, util.WARD_NUMBER, util.ZIP, ZIP_CODE_MAP )

    # Count total residential units and total parcels in each ward
    parcel_count_col_name = util.PARCEL_COUNT + parcel_count_suffix
    for ward, df_ward in df_details.groupby( by=[util.WARD_NUMBER] ):
        summary_row_index = df_summary.loc[ df_summary[util.WARD_NUMBER] == ward ].index
        df_summary.at[summary_row_index, util.TOTAL_OCCUPANCY] = df_ward[util.TOTAL_OCCUPANCY].sum()
        df_summary.at[summary_row_index, parcel_count_col_name] = len( df_ward )

        for s_fuel in util.HEATING_FUEL_MAP:
            df_fuel = df_ward[ df_ward[util.HEATING_FUEL_DESC] == s_fuel ]
            df_summary.at[summary_row_index, HEATING_FUEL_OCCUPANCY_MAP[s_fuel]] = df_fuel[util.TOTAL_OCCUPANCY].sum()

    # Combine statisics for electric and oil
    df_summary[ELEC_OIL_PARCELS] = df_summary[ELECTRIC_PARCELS] + df_summary[OIL_PARCELS]
    df_summary[ELEC_OIL_OCCUPANCY] = df_summary[ELECTRIC_OCCUPANCY] + df_summary[OIL_OCCUPANCY]

    # Fix datatype
    df_summary[ELEC_OIL_PARCELS] = df_summary[ELEC_OIL_PARCELS].astype(int)
    df_summary[ELEC_OIL_OCCUPANCY] = df_summary[ELEC_OIL_OCCUPANCY].astype(int)
    df_summary[util.TOTAL_OCCUPANCY] = df_summary[util.TOTAL_OCCUPANCY].astype(int)
    df_summary[parcel_count_col_name] = df_summary[parcel_count_col_name].astype(int)
    for s_fuel in util.HEATING_FUEL_MAP:
        df_summary[HEATING_FUEL_OCCUPANCY_MAP[s_fuel]] = df_summary[HEATING_FUEL_OCCUPANCY_MAP[s_fuel]].astype(int)

    return df_summary


# Create one parcels table for each ward
def create_ward_parcels_tables( df_parcels ):

    # Group parcels by wards
    for ward, df_group in df_parcels.groupby( by=[util.WARD_NUMBER] ):

        # Create, sort, and save parcels table for current ward
        df_ward_parcels = df_group[WARD_PARCELS_COLUMNS]
        df_ward_parcels = df_ward_parcels.sort_values( by=[util.PRECINCT_NUMBER, util.TOTAL_OCCUPANCY, util.VISION_ID] )
        util.create_table( 'Ward_{}_ResidentialParcels'.format( ward ), conn, cur, df=df_ward_parcels )

    return


# Create wards summary table
def create_wards_summary_table( df_wards, df_parcels ):

    # Initialize wards summary table
    df_summary = df_wards.copy()

    # Add columns containing per-ward solar and wx permit counts
    for s_permit_type in BUILDING_PERMIT_TYPES:
        df_summary = util.add_permit_counts( df_summary, df_parcels, util.WARD_NUMBER, s_permit_type )

    # Build common fields
    df_summary = add_common_summary_columns( df_summary, df_parcels )

    util.create_table( 'WardSummary', conn, cur, df=df_summary )

    return


# Create wards summary table of unweatherized lean properties
def create_wards_summary_table_lean_nwx( df_wards, df_parcels ):

    # Isolate unweatherized lean properties
    df_lean_nwx = df_parcels[ df_parcels[util.LEAN_ELIGIBILITY].isin( [util.LEAN, util.LEAN_MULTI_FAMILY] ) & df_parcels[util.WX_PERMIT].isna() ]

    # Initialize wards summary table
    df_summary = df_wards.copy()

    # Add columns containing per-ward solar and wx permit counts
    for s_permit_type in BUILDING_PERMIT_TYPES:
        df_summary = util.add_permit_counts( df_summary, df_parcels, util.WARD_NUMBER, s_permit_type )

    # Build common fields
    df_summary = add_common_summary_columns( df_summary, df_lean_nwx )

    util.create_table( 'WardSummary_Lean_Nwx', conn, cur, df=df_summary )

    return


# Create wards summary of small rentals
def create_wards_summary_table_small( df_wards, df_parcels ):

    # Isolate small rental properties
    df_rentals_small = df_parcels[( df_parcels[util.TOTAL_OCCUPANCY] >= 2 ) & ( df_parcels[util.TOTAL_OCCUPANCY] <= 4 )]

    # Initialize small rental summary
    df_summary = df_wards.copy()

    # Count parcels with specific total occupancies
    df_summary = util.add_value_counts( df_summary, df_rentals_small, util.WARD_NUMBER, util.TOTAL_OCCUPANCY, RENTAL_MAP_2_4 )

    # Format suffix indicating all parcels
    all_parcels = util.with_units( '2_4' )

    # Get counts of listed permit types
    for s_permit_type in BUILDING_PERMIT_TYPES:

        # Count permits for small rentals with specific occupancy
        for n_occupancy in RENTAL_MAP_2_4:
            df_occupancy = df_parcels[df_parcels[util.TOTAL_OCCUPANCY] == n_occupancy ].copy()
            df_summary = util.add_permit_counts( df_summary, df_occupancy, util.WARD_NUMBER, s_permit_type, util.with_units( n_occupancy ) )

        # Count permits for all small rentals
        df_summary = util.add_permit_counts( df_summary, df_rentals_small, util.WARD_NUMBER, s_permit_type, all_parcels )

    # Build the rest of the small rental summary
    df_summary = add_common_summary_columns( df_summary, df_rentals_small, parcel_count_suffix=all_parcels )

    # Save small rental summary
    util.create_table( 'WardSummary_Rentals_2_4', conn, cur, df=df_summary )

    return


# Create wards summary of large rentals
def create_wards_summary_table_large( df_wards, df_parcels ):

    # Isolate large rental properties
    df_rentals_large = df_parcels[( df_parcels[util.TOTAL_OCCUPANCY] > 4 ) ]

    # Initialize large rental summary
    df_summary = df_wards.copy()

    # Count parcels with specific total occupancies
    df_summary = util.add_value_counts( df_summary, df_rentals_large, util.WARD_NUMBER, util.TOTAL_OCCUPANCY, RENTAL_MAP_5_7 )

    # Swap positions of last two columns
    cols = df_summary.columns.tolist()
    cols[-1], cols[-2] = cols[-2], cols[-1]
    df_summary = df_summary[cols]

    # Format suffix indicating all parcels
    all_parcels = util.with_units( 4, gt=True )

    # Get counts of listed permit types
    for s_permit_type in BUILDING_PERMIT_TYPES:

        # Count permits for large rentals with specific occupancy
        for n_occupancy in RENTAL_MAP_5_7:
            df_occupancy = df_parcels[df_parcels[util.TOTAL_OCCUPANCY] == n_occupancy ].copy()
            df_summary = util.add_permit_counts( df_summary, df_occupancy, util.WARD_NUMBER, s_permit_type, util.with_units( n_occupancy ) )

        # Count permits for largest rentals
        df_occupancy = df_parcels[df_parcels[util.TOTAL_OCCUPANCY] >= 8].copy()
        df_summary = util.add_permit_counts( df_summary, df_occupancy, util.WARD_NUMBER, s_permit_type, util.with_units( 8, plus=True ) )

        # Count permits for all large rentals
        df_summary = util.add_permit_counts( df_summary, df_rentals_large, util.WARD_NUMBER, s_permit_type, all_parcels )

    # Build the rest of the large rental summary
    df_summary = add_common_summary_columns( df_summary, df_rentals_large, parcel_count_suffix=all_parcels )

    # Count parcels with greatest total occupancies
    df_summary[util.PARCEL_COUNT + util.with_units( 8, plus=True )] = df_summary[util.PARCEL_COUNT + all_parcels] - ( df_summary[util.PARCEL_COUNT + util.with_units( 5 )] + df_summary[util.PARCEL_COUNT + util.with_units( 6 )] + df_summary[util.PARCEL_COUNT + util.with_units( 7 )] )

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

    # Create one parcels table for each ward
    create_ward_parcels_tables( df_parcels )

    # Load wards dataframe
    df_wards = pd.read_sql_table( 'RawWards_L', engine, index_col=util.ID, parse_dates=True )

    # Create wards summary table
    create_wards_summary_table( df_wards, df_parcels )

    # Create wards summary table of unweatherized lean properties
    create_wards_summary_table_lean_nwx( df_wards, df_parcels )

    # Create wards summary of small rentals
    create_wards_summary_table_small( df_wards, df_parcels )

    # Create wards summary of large rentals
    create_wards_summary_table_large( df_wards, df_parcels )

    util.report_elapsed_time()
