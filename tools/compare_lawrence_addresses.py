# Copyright 2023 Energize Andover.  All rights reserved.

import pandas as pd

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('display.width', 1000)

# Column name mappings
VISION_ID = 'vision_id'
MAPPED_VALUE = 'normalized_address'
OLD = MAPPED_VALUE + '_old'
NEW = MAPPED_VALUE + '_new'

# Main program
if __name__ == '__main__':

    # Compare results of two Vision scrapes:
    # Determine whether any Vision IDs are mapped to changed values

    print( '' )
    print( 'Looking for changed mappings from "{}" to "{}"'.format( VISION_ID, MAPPED_VALUE ) )

    # Read old and new CSV files
    df_old = pd.read_csv( './in/Parcels_L_old.csv' )
    df_new = pd.read_csv( '../test/Parcels_L.csv' )

    # Isolate useful columns
    df_old = df_old[[VISION_ID, MAPPED_VALUE]]
    df_new = df_new[[VISION_ID, MAPPED_VALUE]]

    # Select Vision IDs that are common between the two dataframes
    df_common = pd.merge( df_old, df_new, how='inner', on=[VISION_ID] )
    df_common = df_common[[VISION_ID]]

    # Isolate rows with common Vision IDs
    df_old = pd.merge( df_old, df_common, how='inner', on=[VISION_ID] )
    df_new = pd.merge( df_new, df_common, how='inner', on=[VISION_ID] )

    # Select rows with unchanged mappings
    df_same = pd.merge( df_old, df_new, how='inner', on=[VISION_ID,MAPPED_VALUE] )

    # Find Vision IDs with changed mappings
    vids_common = set( df_common[VISION_ID] )
    vids_same = set( df_same[VISION_ID] )
    vids_changed = list( vids_common - vids_same )

    # Report findings
    print( '' )
    print( 'Number of common Vision IDs: {}'.format( len( df_common ) ) )
    print( 'Number of Vision IDs with unchanged mappings: {}'.format( len( df_same ) ) )
    print( 'Number of Vision IDs with changed mappings: {}'.format( len( vids_changed ) ) )

    if len( vids_changed ):

        # Construct dataframe showing changed mappings
        df_changed = pd.DataFrame()
        df_changed[VISION_ID] = vids_changed
        df_changed = pd.merge( df_changed, df_old, how='left' )
        df_changed = df_changed.rename( columns={ MAPPED_VALUE: OLD } )
        df_changed = pd.merge( df_changed, df_new, how='left' )
        df_changed = df_changed.rename( columns={ MAPPED_VALUE: NEW } )
        df_changed = df_changed.sort_values( by=[VISION_ID] )
        df_changed = df_changed.reset_index( drop=True )

        # Report findings
        print( '' )
        print( df_changed )
