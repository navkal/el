# Copyright 2023 Energize Andover.  All rights reserved.

import pandas as pd

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('display.width', 1000)

# Column name mappings
VISION_ID = 'vision_id'
ADDR = 'normalized_address'

# Main program
if __name__ == '__main__':

    # Compare results of two Vision scrapes:  
    # Determine whether any Vision ID has a changed address.

    # Read old and new CSV files
    df_old = pd.read_csv( './in/Parcels_L_old.csv' )
    df_new = pd.read_csv( '../test/Parcels_L.csv' )

    # Isolate useful columns
    df_old = df_old[[VISION_ID, ADDR]]
    df_new = df_new[[VISION_ID, ADDR]]

    # Select Vision IDs that are common between the two dataframes
    df_common = pd.merge( df_old, df_new, how='inner', on=[VISION_ID] )
    df_common = df_common[[VISION_ID]]

    # Isolate rows with common Vision IDs
    df_old = pd.merge( df_old, df_common, how='inner', on=[VISION_ID] )
    df_new = pd.merge( df_new, df_common, how='inner', on=[VISION_ID] )

    # Select rows with unchanged mappings from Vision ID to address
    df_same = pd.merge( df_old, df_new, how='inner', on=[VISION_ID,ADDR] )

    print( 'Number of common Vision IDs: {}'.format( len( df_common ) ) )
    print( 'Number of Vision IDs with unchanged addresses: {}'.format( len( df_same ) ) )
    print( 'Number of Vision IDs with changed addresses: {}'.format( len( df_common ) - len( df_same ) ) )
