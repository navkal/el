# Copyright 2022 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util
import normalize

ADDR = util.NORMALIZED_ADDRESS
STREET_NUMBER = util.NORMALIZED_STREET_NUMBER
STREET_NAME = util.NORMALIZED_STREET_NAME
OCCUPANCY = util.NORMALIZED_OCCUPANCY


def expand_addresses():

    # Extract assessment entries that represent address ranges
    df_ranges = df_assessment.copy()
    df_ranges = df_ranges[ df_ranges[ADDR].str.match( '^\d+ \d+ .*$' ) ]

    # Generate a new dataframe that expands the ranges into individual addresses
    df_expanded = pd.DataFrame( columns=df_ranges.columns )

    for index, row in df_ranges.iterrows():

        # Separate address into address range and street
        address_range = row[ADDR].split()[:2]
        street = ' '.join( row[ADDR].split()[2:] )

        # Iterate over all numbers in the address range
        for num in range( int( address_range[0] ), int( address_range[-1] ) + 1, 2 ):
            new_address = str( num ) + ' ' + street
            new_row = row.copy()
            new_row[ADDR] = new_address
            df_expanded = df_expanded.append( new_row, ignore_index=True )

    return( df_expanded )


# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Correlate Chamber of Commerce entries with commercial assessment data' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve raw Chamber of Commerce table from database
    df_raw = pd.read_sql_table( 'RawChamberOfCommerce', engine, index_col=util.ID, parse_dates=True )

    # Clean up addresses before normalization
    df_raw = normalize.prepare_to_normalize( df_raw, util.LOCATION, ADDR )

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df_raw[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY]] = df_raw.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Retrieve commercial assessment table from database
    assessment_columns = \
    [
        ADDR,
        util.ACCOUNT_NUMBER,
        util.OWNER_NAME,
        util.MADDR_LINE.format( 1 ),
        util.MADDR_CITY,
        util.MADDR_STATE,
        util.MADDR_ZIP_CODE,
        util.LEGAL_REFERENCE_SALE_DATE,
        util.STORY_HEIGHT,
        util.RENTAL_LIVING_UNITS,
        util.ROOF_STRUCTURE,
        util.ROOF_STRUCTURE + util._DESC,
        util.ROOF_COVER,
        util.ROOF_COVER + util._DESC,
        util.HEATING_FUEL,
        util.HEATING_FUEL + util._DESC,
        util.HEATING_TYPE,
        util.HEATING_TYPE + util._DESC,
        util.AC_TYPE,
        util.AC_TYPE + util._DESC,
        util.TOTAL_ASSESSED_VALUE,
        util.LAND_USE_CODE,
        util.LAND_USE_CODE + '_1',
        util.LAND_USE_CODE + util._DESC,
    ]
    df_assessment = pd.read_sql_table( 'Assessment_L_Commercial', engine, index_col=util.ID, columns=assessment_columns, parse_dates=True )

    # Merge
    df_merge = pd.merge( df_raw, df_assessment, how='left', on=[ADDR] )

    # Isolate match failures in new dataframe, to retry the merge with expanded address ranges
    df_failed = df_merge.copy()
    df_failed = df_failed[ df_failed[util.ACCOUNT_NUMBER].isna() ]
    df_failed = df_failed[df_raw.columns]
    df_merge = df_merge.dropna( subset=[util.ACCOUNT_NUMBER] )

    # Create dataframe with address ranges expanded into individual addresses
    df_expanded = expand_addresses()

    # Retry the merge
    df_retry = pd.merge( df_failed, df_expanded, how='left', on=[ADDR] )

    # Append retry result to original merge
    df_chamber = df_merge.append( df_retry, ignore_index=True )

    # Sort on license number
    df_chamber = df_chamber.sort_values( by=[util.LICENSE_NUMBER] )

    # Save final table of Chamber of Commerce members
    util.create_table( 'ChamberOfCommerce', conn, cur, df=df_chamber )

    util.report_elapsed_time()
