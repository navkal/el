# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util
import normalize

ADDR = util.NORMALIZED_ADDRESS
STREET_NUMBER = util.NORMALIZED_STREET_NUMBER
STREET_NAME = util.NORMALIZED_STREET_NAME
OCCUPANCY = util.NORMALIZED_OCCUPANCY
ADDITIONAL = util.NORMALIZED_ADDITIONAL_INFO


# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Clean weatherization data' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve city permits table from database
    df_permits = pd.read_sql_table( 'RawBuildingPermits_Wx', engine, index_col=util.ID, parse_dates=True )

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df_permits[ADDR] = df_permits[util.ADDRESS].str.strip()
    df_permits[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df_permits.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Drop empty columns
    df_permits = df_permits.dropna( axis='columns', how='all' )

    # Create table in database
    util.create_table( 'BuildingPermits_L_Wx', conn, cur, df=df_permits )

    # Retrieve GLCAC projects table from database
    df_projects = pd.read_sql_table( 'RawGlcacProjects', engine, index_col=util.ID, parse_dates=True )

    # Normalize addresses.  Use result_type='expand' to load multiple columns!
    df_projects[ADDR] = df_projects[util.ADDRESS].str.strip()
    df_projects[ADDR] = df_projects[ADDR].str.upper()
    df_projects[ADDR] = df_projects[ADDR].str.split( ' MA ', expand=True )[0]
    df_projects[ADDR] = df_projects[ADDR].str.rsplit( n=1, expand=True )[0]
    df_projects[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df_projects.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Drop empty columns
    df_projects = df_projects.dropna( axis='columns', how='all' )

    # Create table in database
    util.create_table( 'GlcacProjects_L', conn, cur, df=df_projects )

    util.report_elapsed_time()
