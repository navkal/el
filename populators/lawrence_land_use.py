# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

LUC = util.LAND_USE_CODE
IS_RES = 'is_res'

def move_properties( df_from, df_to, b_in ):
    sr_move = df_from.apply( lambda row: ( row[LUC] in ls_res_codes ) if b_in else ( row[LUC] not in ls_res_codes ) , axis=1 )
    idx_res_to_com = sr_move[sr_move].index

    # Add rows to destination
    df_to = df_to.append( df_from.loc[idx_res_to_com], ignore_index=True )
    df_to = df_to.reset_index( drop=True )

    # Remove rows from source
    df_from = df_from.drop( index=idx_res_to_com )
    df_from = df_from.reset_index( drop=True )

    # Clean up
    df_to = df_to.dropna( how='all', axis=1 )
    df_to[util.VISION_ID] = df_to[util.VISION_ID].fillna( 0 ).astype( int )
    df_to = df_to.drop_duplicates( subset=[util.ACCOUNT_NUMBER] )
    df_to = df_to.sort_values( by=[util.ACCOUNT_NUMBER] )

    return df_from, df_to


##########################

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Reclassify misplaced properties based on land use codes' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    parser.add_argument( '-l', dest='luc_filename',  help='Land use codes spreadsheet filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve tables from database
    df_res = pd.read_sql_table( 'Assessment_L_Residential_Merged', engine, index_col=util.ID )
    df_com = pd.read_sql_table( 'Assessment_L_Commercial_Merged', engine, index_col=util.ID )

    # Prepare land use code columns for matching
    df_res[LUC] = util.clean_residential_land_use_codes( df_res[LUC] )
    df_com[LUC] = util.clean_residential_land_use_codes( df_com[LUC] )

    # Retrieve residential codes and prepare for processing
    ls_res_codes = util.get_residential_land_use_codes( args.luc_filename)

    # Move properties from residential to commercial table
    df_res, df_com = move_properties( df_res, df_com, False )

    # Move properties from commercial to residential table
    df_com, df_res = move_properties( df_com, df_res, True )

    # Save corrected tables
    util.create_table( 'Assessment_L_Residential', conn, cur, df=df_res )
    util.create_table( 'Assessment_L_Commercial', conn, cur, df=df_com )

    util.report_elapsed_time()
