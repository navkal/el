# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import util

# Incorporate scraped data from online Vision database into previously merge assessment data
def incorporate_vision_assessment_data( engine, df_assessment, verbose=False ):

    # Read parcel records scraped from Vision database
    df_parcels = pd.read_sql_table( 'Assessment_L_Parcels', engine, index_col=util.ID, parse_dates=True )

    # Partition columns into three groups: intersection and two differences
    p_inter_m = df_parcels.columns.intersection( df_assessment.columns )
    p_minus_m = df_parcels.columns.difference( df_assessment.columns )
    m_minus_p = df_assessment.columns.difference( df_parcels.columns )

    if verbose:
        print( '' )
        print( 'Partitioned columns into three groups:' )
        print( '' )
        print( '> Intersection' )
        print( len( p_inter_m ), 'columns' )
        print( list( p_inter_m ) )
        print( '' )
        print( '> Difference: Vision minus assessment' )
        print( len( p_minus_m ), 'columns' )
        print( list( p_minus_m ) )
        print( '' )
        print( '> Difference: Assessment minus Vision' )
        print( len( m_minus_p ), 'columns' )
        print( list( m_minus_p ) )
        print( '' )

    # Concatenate common columns and drop duplicates, preferring Vision records
    df_assessment_common = df_assessment[p_inter_m]
    df_parcels_common = df_parcels[p_inter_m]
    df_result = pd.concat( [df_assessment_common, df_parcels_common] )

    if verbose:
        print( '' )
        print( 'Concatenated common columns of assessment and Vision tables' )
        print( '' )
        print( 'Asessment common columns' )
        print( df_assessment_common.shape )
        print( '' )
        print( 'Vision common columns' )
        print( df_parcels_common.shape )
        print( '' )
        print( 'Concatenation result' )
        print( df_result.shape )

    df_result = df_result.drop_duplicates( subset=[util.ACCOUNT_NUMBER], keep='last' )

    if verbose:
        print( '' )
        print( 'Dropped duplicates from result' )
        print( df_result.shape )

    # Merge remaining columns from Vision data
    columns_to_merge = [util.ACCOUNT_NUMBER] + list( p_minus_m )
    columns_to_merge.remove( util.IS_RESIDENTIAL )
    df_result = pd.merge( df_result, df_parcels[columns_to_merge], how='left', on=[util.ACCOUNT_NUMBER] )

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
    columns_to_merge = [util.ACCOUNT_NUMBER] + list( m_minus_p )
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
