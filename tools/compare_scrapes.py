# Copyright 2023 Energize Andover.  All rights reserved.

import argparse
import pandas as pd

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('display.width', 1000)

# Column name mappings
VISION_ID = 'vision_id'

_OLD = '_old'
_NEW = '_new'


# Compare old and new mappings from Vision ID to specified column
def compare( df_old, df_new, column ):

    # Isolate columns of interest
    df_old = df_old[[VISION_ID, column]]
    df_new = df_new[[VISION_ID, column]]

    # Isolate rows with common Vision IDs
    df_old = pd.merge( df_old, df_common, how='inner', on=[VISION_ID] )
    df_new = pd.merge( df_new, df_common, how='inner', on=[VISION_ID] )

    # Select rows with unchanged mappings
    df_same = pd.merge( df_old, df_new, how='inner', on=[VISION_ID, column] )

    # Find Vision IDs with changed mappings
    vids_same = set( df_same[VISION_ID] )
    vids_changed = list( vids_common - vids_same )

    # Report findings
    print( '' )
    print( '{}:'.format( column ) )
    print( 'Number of Vision IDs with unchanged mappings: {}'.format( len( df_same ) ) )
    print( 'Number of Vision IDs with changed mappings: {}'.format( len( vids_changed ) ) )

    # Load changes into a dataframe
    df_changed = pd.DataFrame()
    df_changed[VISION_ID] = vids_changed
    col_old = column + _OLD
    col_new = column + _NEW

    if len( vids_changed ):
        df_changed = pd.merge( df_changed, df_old, how='left' )
        df_changed = df_changed.rename( columns={ column: col_old } )
        df_changed = pd.merge( df_changed, df_new, how='left' )
        df_changed = df_changed.rename( columns={ column: col_new } )
    else:
        df_changed[col_old] = None
        df_changed[col_new] = None

    # Return result
    return df_changed


# Main program
if __name__ == '__main__':

    # Compare results of two Vision scrapes:
    # Determine whether any Vision IDs are mapped to changed values

    parser = argparse.ArgumentParser( description='Compare results of two Vision scrapes' )
    parser.add_argument( '-o', dest='old_csv',  help='Old CSV filename', required=True )
    parser.add_argument( '-n', dest='new_csv',  help='New CSV filename', required=True )
    parser.add_argument( '-c', dest='columns',  help='List of columns to include in comparison', required=True )
    parser.add_argument( '-r', dest='result_xl',  help='Result Excel filename', required=True )

    args = parser.parse_args()

    # Read old and new CSV files
    df_old = pd.read_csv( args.old_csv )
    df_new = pd.read_csv( args.new_csv )

    # Select Vision IDs that are common between the two dataframes
    df_common = pd.merge( df_old, df_new, how='inner', on=[VISION_ID] )
    df_common = df_common[[VISION_ID]]
    vids_common = set( df_common[VISION_ID] )

    # Compare old and new with respect to specified column
    ls_columns = args.columns.split( ',' )
    print( 'Comparing columns {} in {} and {}:'.format( ls_columns, args.old_csv, args.new_csv ) )
    df_result = df_common
    for column in ls_columns:
        df_changed = compare( df_old, df_new, column )
        df_result = pd.merge( df_result, df_changed, on=[VISION_ID], how='outer' )

    # Isolate rows with changed mappings
    drop_subset = set( df_result.columns ) - set( [VISION_ID] )
    df_result = df_result.dropna( subset=drop_subset, how='all' )
    df_result = df_result.dropna( axis='columns', how='all' )
    df_result = df_result.sort_values( by=[VISION_ID] )
    df_result = df_result.reset_index( drop=True )
    df_result = df_result.fillna( '' )

    print( '' )
    print( df_result )

    df_result.to_excel( args.result_xl, index=False )

