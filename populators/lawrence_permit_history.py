# Copyright 2023 Energize Lawrence.  All rights reserved.
# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util
import normalize
import vision

ADDR = util.NORMALIZED_ADDRESS
PERM = util.PERMIT_NUMBER
ACCT = util.ACCOUNT_NUMBER

PERMIT_SUFFIXES = \
[
    '_Wx',
    '_Solar',
]

def merge_permit_numbers( df_parcels, permit_suffix ):

    # Read specified table of building permits
    df_permits = pd.read_sql_table( 'BuildingPermits_L' + permit_suffix, engine, index_col=util.ID, parse_dates=True )

    # Format column name for permits of current table
    perm_col_name = PERM + permit_suffix.lower()

    # Isolate columns of permit table to be merged
    df_permits = df_permits.rename( columns={ PERM: perm_col_name } )
    df_permits = df_permits[ [ADDR, perm_col_name] ]

    # Merge permits to parcels
    df_parcels = pd.merge( df_parcels, df_permits, on=[ADDR], how='left' )

    # Iterate over parcel account number groups
    for idx, df_group in df_parcels.groupby( by=[ACCT] ):

        # If this account number appears multiple times, collect all permit numbers into one string
        if len( df_group ) > 1:

            # List all permit numbers associated with current account number
            ls_permits = []
            for s_permit in df_group[perm_col_name]:
                if pd.notnull( s_permit ) and s_permit not in ls_permits:
                    ls_permits.append( s_permit )
            s_permits = ', '.join( ls_permits )

            # Determine which rows to keep and drop
            keep_index = df_group.index[0:1]
            drop_index = df_group.index[1:]

            # Save permit number list in row to be kept; and drop other rows
            df_parcels.at[keep_index, perm_col_name] = s_permits
            df_parcels = df_parcels.drop( index=drop_index )

    return df_parcels


##################################

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Correlate data in commercial assessment tables' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve raw parcels table from database
    df_parcels = pd.read_sql_table( 'RawParcels', engine, index_col=util.ID, parse_dates=True )

    # Drop duplicate account numbers
    df_parcels = df_parcels.drop_duplicates( subset=[ACCT], keep='last' )

    # Merge permit numbers from specified permit tables
    for s_suffix in PERMIT_SUFFIXES:
        df_parcels = merge_permit_numbers( df_parcels, s_suffix )

    # Sort on account number
    df_parcels = df_parcels.sort_values( by=[ACCT] )

    # Save final table of commercial assessments
    util.create_table( 'Assessment_L_Parcels', conn, cur, df=df_parcels )

    util.report_elapsed_time()
