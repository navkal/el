# Copyright 2023 Energize Lawrence.  All rights reserved.

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import re

import numpy as np

import util



URL_BASE = 'https://gis.vgsi.com/{}ma/parcel.aspx?pid='

BUILDING_TABLE_ID_FORMAT = 'MainContent_ctl{:02d}_grdCns'
BUILDING_AREA_ID_FORMAT = 'MainContent_ctl{:02d}_lblBldArea'
BUILDING_YEAR_ID_FORMAT = 'MainContent_ctl{:02d}_lblYearBuilt'
BUILDING_TABLE_ID = 'building_id'
BUILDING_AREA_ID = 'area_id'
BUILDING_YEAR_ID = 'year_id'

#
# Utility functions to clean Vision data
#

def clean_string( col, remove_all_spaces=False ):
    col = col.fillna( '' ).astype( str )
    col = col.str.strip().replace( r'\s+', ' ', regex=True )
    if remove_all_spaces:
        col = col.str.strip().replace( r'\s', '', regex=True )
    return col

def clean_float( col ):
    col = col.str.strip().replace( r'^\(', '', regex=True ).replace( r'\)$', '', regex=True )
    col = col.replace( r'^\s*$', np.nan, regex=True)
    col = col.fillna( '0' ).astype( float )
    return col

def clean_integer( col ):
    col = col.replace( '[\$,]', '', regex=True )
    col = clean_float( col ).astype( int )
    return col

def clean_date( col ):
    col = pd.to_datetime( col, infer_datetime_format=True, errors='coerce' )
    return col


# Scrape HTML element by id
def scrape_element( soup, tag, id ):
    element = soup.find( tag, id=id )
    text = element.string if element else ''
    return text

# Scrape multi-line HTML element
def scrape_lines( soup, tag, id, sep=', ' ):
    # Initialize list of lines
    ls_lines = []

    # Find and iterate over lines
    lines = soup.find( tag, id=id )
    for line in lines:
        s = line.string
        if s:
            s = s.strip()
            if s:
                ls_lines.append( s.strip() )

    # Join lines, delimited by separator
    text = sep.join( ls_lines )

    return text

# Scrape multi-line address
def scrape_address( soup, tag, id ):
    address = scrape_lines( soup, tag, id )
    match = re.search( r'\d{5}(-\d{4})?$', address )
    zip = match.group() if match else ''
    return address, zip


# Find HTML IDs associated with building tables, areas, and years
def find_all_building_ids( soup, building_count ):

    ls_building_ids = []
    first_building_id = ''
    first_area_id = ''
    first_year_id = ''

    # Set range limit for the search
    try:
        range_max = min( 5 + ( 3 * int( building_count ) ), 100 )
    except:
        range_max = 100

    # Search for HTML IDs, using 2-digit integers from 01 to range max
    for n_index in range( 1, range_max ):

        # Initialize dictionary of building and area IDs
        dc_ids = { BUILDING_TABLE_ID: '', BUILDING_AREA_ID: '' }

        # If page contains building table with current index, save the ID
        building_id = BUILDING_TABLE_ID_FORMAT.format( n_index )
        if soup.find( 'table', id=building_id ):
            dc_ids[BUILDING_TABLE_ID] = building_id
            if first_building_id == '':
                first_building_id = building_id

        # If page contains building area with current index, save the ID
        area_id = BUILDING_AREA_ID_FORMAT.format( n_index )
        if soup.find( 'span', id=area_id ):
            dc_ids[BUILDING_AREA_ID] = area_id
            if first_area_id == '':
                first_area_id = area_id

        # If page contains year built with current index, save the ID
        year_id = BUILDING_YEAR_ID_FORMAT.format( n_index )
        if soup.find( 'span', id=year_id ):
            if first_year_id == '':
                first_year_id = year_id

        # If we got anything in the dictionary, append to the list
        if dc_ids[BUILDING_TABLE_ID] or dc_ids[BUILDING_AREA_ID]:
            ls_building_ids.append( dc_ids )

    return ls_building_ids, first_building_id, first_area_id, first_year_id



# Incorporate scraped data from online Vision database into previously merged assessment data
def incorporate_vision_assessment_data( engine, df_assessment, verbose=False ):

    # Read parcel records scraped from Vision database
    df_vision = pd.read_sql_table( 'Parcels_L', engine, index_col=util.ID, parse_dates=True )

    # Partition columns into three groups: intersection and two differences
    v_inter_a = df_vision.columns.intersection( df_assessment.columns )
    v_minus_a = df_vision.columns.difference( df_assessment.columns )
    a_minus_v = df_assessment.columns.difference( df_vision.columns )

    if verbose:
        print( '' )
        print( 'Partitioned columns into three groups:' )
        print( '' )
        print( '> Intersection' )
        print( len( v_inter_a ), 'columns' )
        print( list( v_inter_a ) )
        print( '' )
        print( '> Difference: Vision minus assessment' )
        print( len( v_minus_a ), 'columns' )
        print( list( v_minus_a ) )
        print( '' )
        print( '> Difference: Assessment minus Vision' )
        print( len( a_minus_v ), 'columns' )
        print( list( a_minus_v ) )
        print( '' )

    # Concatenate common columns and drop duplicates, preferring Vision records
    df_vision_common = df_vision[v_inter_a]
    df_assessment_common = df_assessment[v_inter_a]
    df_result = pd.concat( [df_vision_common, df_assessment_common] )

    if verbose:
        print( '' )
        print( 'Concatenated common columns of Vision and assessment tables' )
        print( '' )
        print( 'Vision common columns' )
        print( df_vision_common.shape )
        print( '' )
        print( 'Asessment common columns' )
        print( df_assessment_common.shape )
        print( '' )
        print( 'Concatenation result' )
        print( df_result.shape )

    df_result = df_result.drop_duplicates( subset=[util.ACCOUNT_NUMBER], keep='first' )

    if verbose:
        print( '' )
        print( 'Dropped duplicates from result' )
        print( df_result.shape )

    # Merge remaining columns from Vision data
    columns_to_merge = [util.ACCOUNT_NUMBER] + list( v_minus_a )
    columns_to_merge.remove( util.IS_RESIDENTIAL )
    df_result = pd.merge( df_result, df_vision[columns_to_merge], how='left', on=[util.ACCOUNT_NUMBER] )

    if verbose:
        print( '' )
        print( 'Merged Vision columns to result' )
        print( df_result.shape )

    df_result = df_result.drop_duplicates( subset=[util.ACCOUNT_NUMBER], keep='last' )

    if verbose:
        print( '' )
        print( 'Dropped duplicates from result' )
        print( df_result.shape )

    # Merge remaining columns from previously merged commercial data
    columns_to_merge = [util.ACCOUNT_NUMBER] + list( a_minus_v )
    df_result = pd.merge( df_result, df_assessment[columns_to_merge], how='left', on=[util.ACCOUNT_NUMBER] )

    if verbose:
        print( '' )
        print( 'Merged assessment columns to result' )
        print( df_result.shape )

    df_result = df_result.drop_duplicates( subset=[util.ACCOUNT_NUMBER], keep='last' )

    if verbose:
        print( '' )
        print( 'Dropped duplicates from result' )
        print( df_result.shape )

    # Convert Vision ID to integer
    df_result[util.VISION_ID] = df_result[util.VISION_ID].fillna( 0 ).astype( int )

    # Sort on account number
    df_result = df_result.sort_values( by=[util.ACCOUNT_NUMBER] )

    return df_result
