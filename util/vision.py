# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import util

# Incorporate scraped data from online Vision database into previously merged assessment data
def incorporate_vision_assessment_data( engine, df_assessment, verbose=False ):

    # Read parcel records scraped from Vision database
    df_vision = pd.read_sql_table( 'RawParcels', engine, index_col=util.ID, parse_dates=True )

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
