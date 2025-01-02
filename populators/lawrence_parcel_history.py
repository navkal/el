# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

ACCT = util.ACCOUNT_NUMBER


# Merge permit numbers of specified type to parcels dataframe
def merge_permit_numbers( df_parcels, permit_type ):

    # Read specified table of building permits
    df_permits = pd.read_sql_table( 'BuildingPermits_L' + '_' + permit_type.capitalize(), engine, index_col=util.ID, parse_dates=True )

    # Determine names of old, source column and new, output column
    old_col_name = util.PERMIT_NUMBER
    new_col_name = permit_type + util._PERMIT

    # Do the merge
    df_parcels = merge_to_parcels_table( df_parcels, df_permits, old_col_name, new_col_name )

    # Return result
    return df_parcels


# Merge GLCAC job numbers to parcels dataframe
def merge_glcac_job_numbers( df_parcels ):

    # Read specified table of building permits
    df_jobs = pd.read_sql_table( 'GlcacJobs_L', engine, index_col=util.ID, parse_dates=True )

    # Determine names of old, source column and new, output column
    old_col_name = util.JOB_NUMBER
    new_col_name = 'glcac_job'

    # Do the merge
    df_parcels = merge_to_parcels_table( df_parcels, df_jobs, old_col_name, new_col_name )

    # Return result
    return df_parcels


# Merge National Grid accounts to parcels dataframe
def merge_national_grid_accounts( df_parcels ):

    # Read specified table of National Grid accounts
    df_accounts = pd.read_sql_table( 'NationalGridMeters_L', engine, index_col=util.ID, parse_dates=True )

    # Determine names of old, source column and new, output column
    old_col_name = util.ACCOUNT
    new_col_name = 'ng_account'

    # Convert to string
    df_accounts[old_col_name] = df_accounts[old_col_name].astype( str )

    # Do the merge
    df_parcels = merge_to_parcels_table( df_parcels, df_accounts, old_col_name, new_col_name )

    # Return result
    return df_parcels


# Perform specified merge to parcels table
def merge_to_parcels_table( df_parcels, df_permits, old_col_name, new_col_name ):

    # Isolate columns of permit table to be merged
    df_permits = df_permits.rename( columns={ old_col_name: new_col_name } )
    df_permits = df_permits[ [ACCT, new_col_name] ]

    # Merge permits to parcels
    df_parcels = pd.merge( df_parcels, df_permits, on=[ACCT], how='left' )

    # Iterate over parcel account number groups
    for idx, df_group in df_parcels.groupby( by=[ACCT] ):

        # If this account number appears multiple times, collect all permit numbers into one string
        if len( df_group ) > 1:

            # List all permit numbers associated with current account number
            ls_permits = []
            for s_permit in df_group[new_col_name]:
                if pd.notnull( s_permit ) and s_permit not in ls_permits:
                    ls_permits.append( s_permit )
            ls_permits.sort()
            s_permits = ', '.join( ls_permits )

            # Determine which rows to keep and drop
            keep_index = df_group.index[0:1]
            drop_index = df_group.index[1:]

            # Save permit number list in row to be kept; and drop other rows
            df_parcels.at[keep_index, new_col_name] = s_permits
            df_parcels = df_parcels.drop( index=drop_index )

    return df_parcels


##################################

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Merge permit information into parcels table' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve parcels table from database
    df_parcels = pd.read_sql_table( 'Parcels_L', engine, index_col=util.ID, parse_dates=True )

    # Merge permit numbers from specified permit tables
    for s_type in util.BUILDING_PERMIT_TYPES:
        df_parcels = merge_permit_numbers( df_parcels, s_type )

    # Merge GLCAC job numbers
    df_parcels = merge_glcac_job_numbers( df_parcels )

    # Merge National Grid accounts
    df_parcels = merge_national_grid_accounts( df_parcels )

    # Sort on account number
    df_parcels = df_parcels.sort_values( by=[ACCT] )

    # Save final table of commercial assessments
    util.create_table( 'Assessment_L_Parcels', conn, cur, df=df_parcels )

    util.report_elapsed_time()
