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
ADDITIONAL = util.NORMALIZED_ADDITIONAL_INFO

SAVE_ADDR = 'save_normalized_address'


# Isolate entries that did not find matches in the merge
def isolate_unmatched( df_merge ):

    # Create dataframe of unmatched entries
    df_unmatched = df_merge.copy()
    df_unmatched = df_unmatched[ df_unmatched[util.ACCOUNT_NUMBER].isna() ]
    df_unmatched = df_unmatched[df_raw.columns]

    # Clear unmatched entries out of merged data
    df_matched = df_merge.dropna( subset=[util.ACCOUNT_NUMBER] )

    return df_matched, df_unmatched


# Expand address ranges into entries that represent all addresses in the range
def expand_address_ranges( df ):

    # Extract entries that represent address ranges
    df_ranges = df.copy()
    df_ranges = df_ranges[ df_ranges[ADDR].str.match( '^\d+-\d+ .*$' ) ]

    # Generate a new dataframe that expands the ranges into individual addresses
    df_expanded = pd.DataFrame( columns=df_ranges.columns )

    for index, row in df_ranges.iterrows():

        # Separate address into address range and street
        address_range = row[ADDR].split()[0].split( '-' )
        street = ' '.join( row[ADDR].split()[1:] )

        # Iterate over all numbers in the address range
        for num in range( int( address_range[0] ), int( address_range[-1] ) + 1, 2 ):
            new_address = str( num ) + ' ' + street
            new_row = row.copy()
            new_row[ADDR] = new_address
            df_expanded = df_expanded.append( new_row, ignore_index=True )

    df_no_ranges = df.loc[ df.index.difference( df_ranges.index ) ]
    df_expanded = df_expanded.append( df_no_ranges, ignore_index=True )

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

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df_raw[ADDR] = df_raw[util.LOCATION]
    df_raw[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df_raw.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

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

    # Isolate unmatched entries in new dataframe, to retry the merge with expanded address ranges on right
    df_matched, df_unmatched = isolate_unmatched( df_merge )
    df_result = df_matched

    # Create dataframe with address ranges expanded into individual addresses
    df_expanded_right = expand_address_ranges( df_assessment )

    # Retry the merge with expanded addresses on right
    df_merge = pd.merge( df_unmatched, df_expanded_right, how='left', on=[ADDR] )

    # Isolate unmatched entries in new dataframe, to retry the merge with expanded address ranges on left
    df_matched, df_unmatched = isolate_unmatched( df_merge )
    df_result = df_result.append( df_matched, ignore_index=True )

    # Create dataframe with address ranges expanded into individual addresses
    df_unmatched[SAVE_ADDR] = df_unmatched[ADDR]
    df_expanded_left = expand_address_ranges( df_unmatched )

    # Re-retry the merge with expanded addresses on left
    df_merge = pd.merge( df_expanded_left, df_assessment, how='left', on=[ADDR] )
    df_merge[ADDR] = df_merge[SAVE_ADDR]
    df_merge = df_merge.drop( columns=[SAVE_ADDR] )

    # Isolate unmatched entries in new dataframe, to retry the merge with expanded address ranges on left
    df_matched, df_unmatched = isolate_unmatched( df_merge )
    df_result = df_result.append( df_matched, ignore_index=True )

    # Append unmatched entries
    df_result = df_result.append( df_unmatched, ignore_index=True )

    # Drop duplicates
    df_result = df_result.drop_duplicates()

    # Sort on license number
    df_result = df_result.sort_values( by=[util.LICENSE_NUMBER, util.ACCOUNT_NUMBER] )

    # Save final table of Chamber of Commerce members
    util.create_table( 'ChamberOfCommerce', conn, cur, df=df_result )

    util.report_elapsed_time()
