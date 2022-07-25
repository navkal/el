# Copyright 2022 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import re

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

PROPERTY_TYPE = 'property_type'
COMMERCIAL = 'Commercial'
RESIDENTIAL = 'Residential'

# Isolate entries that did not find matches in the merge
def isolate_unmatched( df_merge, df_result, property_type ):

    # Create dataframe of unmatched entries
    df_unmatched = df_merge.copy()
    df_unmatched = df_unmatched[ df_unmatched[util.ACCOUNT_NUMBER].isna() ]
    df_unmatched = df_unmatched[df_raw.columns]

    # Clear unmatched entries out of merged data
    df_matched = df_merge.copy()
    df_matched = df_matched.dropna( subset=[util.ACCOUNT_NUMBER] )

    # Append matched entries to the result
    df_matched[PROPERTY_TYPE] = property_type
    df_result = df_result.append( df_matched, ignore_index=True )

    # Report progress
    print( '---' )
    print( property_type )
    print( 'Matched: {}, Unmatched: {}'.format( df_matched.shape, df_unmatched.shape ) )
    print( 'Result: {}'.format( df_result.shape ) )

    return df_result, df_unmatched


# Expand address ranges into entries that represent all addresses in the range
def expand_address_ranges( df ):

    # Extract entries that represent address ranges
    df_ranges = df.copy()
    df_ranges = df_ranges[ df_ranges[ADDR].str.match( '^\d+[A-Z]*-\d+[A-Z]* .*$' ) ]

    # Generate a new dataframe that expands the ranges into individual addresses
    df_expanded = pd.DataFrame( columns=df_ranges.columns )

    for index, row in df_ranges.iterrows():

        # Extract numeric address range
        address_range = row[ADDR].split()[0].split( '-' )
        range_start = int( re.search( '^\d*', address_range[0] ).group(0) )
        range_end = int( re.search( '^\d*', address_range[1] ).group(0) ) + 1

        # Extract street
        street = ' '.join( row[ADDR].split()[1:] )

        # Iterate over all numbers in the address range
        for num in range( range_start, range_end, 2 ):
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
    parser = argparse.ArgumentParser( description='Correlate business entries with assessment data' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve raw Businesses table from database
    df_bus_1 = pd.read_sql_table( 'RawBusinesses_1', engine, index_col=util.ID, parse_dates=True )
    df_bus_2 = pd.read_sql_table( 'RawBusinesses_2', engine, index_col=util.ID, parse_dates=True, columns=[util.LICENSE_NUMBER, util.BUSINESS_MANAGER] )
    df_raw = pd.merge( df_bus_1, df_bus_2, how='left', on=util.LICENSE_NUMBER )

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
    not_in_residential = \
    [
        util.RENTAL_LIVING_UNITS,
        util.HEATING_FUEL + util._DESC,
        util.AC_TYPE + util._DESC,
        util.LAND_USE_CODE + '_1',
        util.LAND_USE_CODE + util._DESC,
    ]
    df_assessment_com = pd.read_sql_table( 'Assessment_L_Commercial', engine, index_col=util.ID, columns=assessment_columns, parse_dates=True )
    assessment_columns = list( set( assessment_columns ) - set( not_in_residential ) )
    df_assessment_res = pd.read_sql_table( 'Assessment_L_Residential', engine, index_col=util.ID, columns=assessment_columns, parse_dates=True )

    print( '---' )
    print( 'Raw: {}'.format( df_raw.shape ) )

    # Initialize empty result
    df_result = pd.DataFrame()
    df_raw[SAVE_ADDR] = df_raw[ADDR]
    df_unmatched = df_raw.copy()

    # Merge unmatched with commercial
    df_merge = pd.merge( df_unmatched, df_assessment_com, how='left', on=[ADDR] )
    df_result, df_unmatched = isolate_unmatched( df_merge, df_result, COMMERCIAL )

    # Merge unmatched with residential
    df_merge = pd.merge( df_unmatched, df_assessment_res, how='left', on=[ADDR] )
    df_result, df_unmatched = isolate_unmatched( df_merge, df_result, RESIDENTIAL )

    # Expand addresses in assessment data
    df_assessment_com = expand_address_ranges( df_assessment_com )
    df_assessment_res = expand_address_ranges( df_assessment_res )

    # Merge unmatched with commercial
    df_merge = pd.merge( df_unmatched, df_assessment_com, how='left', on=[ADDR] )
    df_result, df_unmatched = isolate_unmatched( df_merge, df_result, COMMERCIAL )

    # Merge unmatched with residential
    df_merge = pd.merge( df_unmatched, df_assessment_res, how='left', on=[ADDR] )
    df_result, df_unmatched = isolate_unmatched( df_merge, df_result, RESIDENTIAL )

    # Expand addresses in unmatched business data
    df_unmatched = expand_address_ranges( df_unmatched )

    # Merge unmatched with commercial
    df_merge = pd.merge( df_unmatched, df_assessment_com, how='left', on=[ADDR] )
    df_merge[ADDR] = df_merge[SAVE_ADDR]
    df_result, df_unmatched = isolate_unmatched( df_merge, df_result, COMMERCIAL )

    # Merge unmatched with residential
    df_merge = pd.merge( df_unmatched, df_assessment_res, how='left', on=[ADDR] )
    df_merge[ADDR] = df_merge[SAVE_ADDR]
    df_result, df_unmatched = isolate_unmatched( df_merge, df_result, RESIDENTIAL )

    # Finish up
    df_result = df_result.append( df_unmatched, ignore_index=True )
    df_result = df_result.drop( columns=[SAVE_ADDR] )
    df_result = df_result.drop_duplicates()
    df_result = df_result.sort_values( by=[util.LICENSE_NUMBER, util.ACCOUNT_NUMBER] )

    # Report final statistics
    print( '---' )
    len_result = len( df_result )
    len_unmatched = len( df_result[df_result[util.ACCOUNT_NUMBER].isna() ] )
    len_matched = len_result - len_unmatched
    print( 'FINAL Matched: {}'.format( len_matched ) )
    print( 'FINAL Unmatched: {}'.format( len_unmatched ) )
    print( 'FINAL Result: {}'.format( len_result ) )

    # Save final table of businesses
    util.create_table( 'Businesses_L', conn, cur, df=df_result )

    util.report_elapsed_time()
