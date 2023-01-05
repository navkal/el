# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

##########################

LUC = util.LAND_USE_CODE

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
    df_res = pd.read_sql_table( 'Assessment_L_Residential', engine, index_col=util.ID )
    df_com = pd.read_sql_table( 'Assessment_L_Commercial', engine, index_col=util.ID )
    df_res_codes = pd.read_excel( args.luc_filename, dtype=object )

    print( df_res[ [LUC] ] )
    print( df_com[ [LUC] ] )
    print( df_res_codes )
    sr_res_codes = df_res_codes['Use Code'].append( df_res_codes['Alt Use Code'] )
    print( len( sr_res_codes ) )
    sr_res_codes = sr_res_codes.drop_duplicates()
    print( len( sr_res_codes ) )
    print( sr_res_codes )
    sr_res_codes = sr_res_codes.astype(str).str.zfill( 4 )
    print( sr_res_codes )
    ls_res_codes = list( sr_res_codes )
    print( ls_res_codes )

    df_res['is_res'] = df_res.apply( lambda row: ( util.YES if ( row[LUC] in ls_res_codes ) else util.NO ), axis=1 )
    df_com['is_res'] = df_com.apply( lambda row: ( util.YES if ( row[LUC] in ls_res_codes ) else util.NO ), axis=1 )

    print( df_res[ [LUC, 'is_res' ] ] )
    print( df_com[ [LUC, 'is_res' ] ] )













    # Save corrected tables
    # util.create_table( 'Assessment_L_Residential', conn, cur, df=df_res )
    # util.create_table( 'Assessment_L_Commercial', conn, cur, df=df_com )

    util.report_elapsed_time()
