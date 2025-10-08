# Copyright 2025 Energize Andover.  All rights reserved.

import argparse
import os

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.max_rows', 500 )
pd.set_option( 'display.width', 1000 )

import sqlite3
import sqlalchemy

import sys
sys.path.append( '../util' )
import util

DT = 'date_time'
Y = 'year'
Q = 'quarter'
YQ = 'year_quarter'
PERIOD = 'period'
WX_COUNT = 'wx_count'
WX_COUNT_LEAN = 'wx_count_lean'

FUELS = \
[
    util.ELECTRIC,
    util.OIL,
    util.GAS,
]
_LEAN = '_lean'

BN = util.BUSINESS_NAME
TPC = 'tot_' + util.PROJECT_COST
APC = 'avg_' + util.PROJECT_COST



# Add rows to the weatherization summary
def add_wx_summary_rows( df_permits, df_summary, s_period_column, s_business_name = None ):

    # Group by specified column representing a period of time
    for idx, df_group in df_permits.groupby( by=[s_period_column] ):

        # Initialize row with attribute name and total weatherization count
        summary_row = {}
        summary_row[PERIOD] = idx
        summary_row[WX_COUNT] = len( df_group )

        # Calculate weatherization subtotals per fuel type
        for s_fuel in FUELS:
            summary_row[s_fuel.lower()] = len( df_group[df_group[util.HEATING_FUEL_DESC] == s_fuel] )

        # Isolate and count permits at LEAN-eligible parcels
        df_group = df_group[df_group[util.LEAN_ELIGIBILITY] == util.LEAN]
        summary_row[WX_COUNT_LEAN] = len( df_group )

        # Calculate LEAN weatherization subtotals per fuel type
        for s_fuel in FUELS:
            s = s_fuel.lower()
            s_lean = s + _LEAN
            summary_row[s_lean] = len( df_group[df_group[util.HEATING_FUEL_DESC] == s_fuel] )

        # Optionally save business name in row and calculate project cost statistics
        if s_business_name:
            summary_row[BN] = s_business_name
            summary_row[TPC] = df_group[util.PROJECT_COST].sum()
            summary_row[APC] = round( summary_row[TPC] / summary_row[WX_COUNT], 2 )

        # Append completed row to summary dataframe
        df_summary = df_summary.append( summary_row, ignore_index=True )

    return df_summary


# Summarize weatherization permit counts by period
def make_wx_summary_by_period( conn, cur, engine, output_directory ):

    # Read extended table of weatherization permits from database
    df_permits = pd.read_sql_table( 'BuildingPermits_L_Wx_Extended', engine, index_col=util.ID, parse_dates=True )

    # Isolate and sort usable rows
    df_permits = df_permits.dropna( subset=[util.DATE_ISSUED] )
    df_permits = df_permits[df_permits[util.HEATING_FUEL_DESC].isin( FUELS )]
    df_permits = df_permits.sort_values( by=[util.DATE_ISSUED] )

    # Add period column indicating year and quarter
    df_permits[Y] = df_permits[util.DATE_ISSUED].str.split( '-', expand=True )[0]
    df_permits[DT] = pd.to_datetime( df_permits[util.DATE_ISSUED] )
    df_permits[Q] = df_permits[DT].dt.quarter.astype(str)
    df_permits[YQ] = df_permits[Y] + ' Q' + df_permits[Q]

    # Initialize summary column order
    summary_columns = [PERIOD, WX_COUNT]
    for s_fuel in FUELS:
        summary_columns.append( s_fuel.lower() )
    summary_columns.append( WX_COUNT_LEAN )
    for s_fuel in FUELS:
        summary_columns.append( s_fuel.lower() + _LEAN )

    # Initialize empty summary dataframe
    df_summary = pd.DataFrame( columns=summary_columns )

    # Add rows per year
    df_summary = add_wx_summary_rows( df_permits, df_summary, Y )

    # Add rows per quarter
    df_summary = add_wx_summary_rows( df_permits, df_summary, YQ )

    # Fix numeric types
    for s_fuel in FUELS:
        s = s_fuel.lower()
        s_lean = s + _LEAN
        df_summary[s] = df_summary[s].astype( int )
        df_summary[s_lean] = df_summary[s_lean].astype( int )

    # Sort on period column
    df_summary = df_summary.sort_values( by=[PERIOD] )

    # Save to database and excel file
    s_table_name = 'WxSummaryByPeriod'
    util.create_table( s_table_name, conn, cur, df=df_summary )
    df_summary.to_excel( os.path.join( output_directory, s_table_name + '.xlsx' ), index=False )


# Summarize weatherization permit counts by contractor
def make_wx_summary_by_contractor( conn, cur, engine, output_directory ):

    # Read extended table of weatherization permits from database
    df_permits = pd.read_sql_table( 'BuildingPermits_L_Wx_Extended', engine, index_col=util.ID, parse_dates=True )

    # Isolate and sort usable rows
    df_permits = df_permits.dropna( subset=[util.DATE_ISSUED] )
    df_permits = df_permits[df_permits[util.HEATING_FUEL_DESC].isin( FUELS )]
    df_permits = df_permits.sort_values( by=[util.DATE_ISSUED] )

    # Add period column indicating year and quarter
    df_permits[Y] = df_permits[util.DATE_ISSUED].str.split( '-', expand=True )[0]
    df_permits[DT] = pd.to_datetime( df_permits[util.DATE_ISSUED] )
    df_permits[Q] = df_permits[DT].dt.quarter.astype(str)
    df_permits[YQ] = df_permits[Y] + ' Q' + df_permits[Q]

    # Include most recent 8 quarters in summary
    sr_yq = df_permits[YQ].sort_values().unique()
    sr_yq = list( sr_yq )[-8:]
    df_permits = df_permits[ df_permits[YQ].isin( sr_yq ) ]

    # Initialize summary column order
    summary_columns = [PERIOD, BN, TPC, APC, WX_COUNT]
    for s_fuel in FUELS:
        summary_columns.append( s_fuel.lower() )
    summary_columns.append( WX_COUNT_LEAN )
    for s_fuel in FUELS:
        summary_columns.append( s_fuel.lower() + _LEAN )

    # Initialize empty summary dataframe
    df_summary = pd.DataFrame( columns=summary_columns )

    # Iterate over contractor business names
    for idx, df_group in df_permits.groupby( by=[BN] ):

        # Build summary dataframe pertaining to current contractor
        df_contractor = pd.DataFrame( columns=summary_columns )
        df_contractor = add_wx_summary_rows( df_group, df_contractor, Y, idx )
        df_contractor = add_wx_summary_rows( df_group, df_contractor, YQ, idx )

        # Append current contractor summary to overall summary
        df_summary = df_summary.append( df_contractor, ignore_index=True )

    # Fix numeric types
    for s_fuel in FUELS:
        s = s_fuel.lower()
        s_lean = s + _LEAN
        df_summary[s] = df_summary[s].astype( int )
        df_summary[s_lean] = df_summary[s_lean].astype( int )

    # Sort
    df_summary = df_summary.sort_values( by=[BN, PERIOD] )

    # Save to database and excel file
    s_table_name = 'WxSummaryByContractor'
    util.create_table( s_table_name, conn, cur, df=df_summary )
    df_summary.to_excel( os.path.join( output_directory, s_table_name + '.xlsx' ), index=False )


######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate summaries based on contents of master database' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename', required=True )
    parser.add_argument( '-o', dest='output_directory', help='Target directory output files', required=True )
    parser.add_argument( '-c', dest='clear_directory', action='store_true', help='Clear target directory first?' )

    args = parser.parse_args()

    # Optionally clear target directory
    if args.clear_directory:
        util.clear_directory( args.output_directory )

    # Report arguments
    print( '' )
    print( 'Arguments' )
    print( ' Input database:', args.master_filename )
    print( ' Output directory:', args.output_directory )


    # Open the database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Summarize weatherization permit data
    make_wx_summary_by_period( conn, cur, engine, args.output_directory )
    make_wx_summary_by_contractor( conn, cur, engine, args.output_directory )

    util.report_elapsed_time()
