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






# Add rows to the per-period weatherization summary
def add_wx_summary_rows( df_permits, df_summary, period ):

    for idx, df_group in df_permits.groupby( by=[period] ):

        # Initialize row with period and total weatherization count
        report_row = {}
        report_row[PERIOD] = idx
        report_row[WX_COUNT] = len( df_group )

        # Calculate weatherization subtotals per fuel type
        for s_fuel in FUELS:
            report_row[s_fuel.lower()] = len( df_group[df_group[util.HEATING_FUEL_DESC] == s_fuel] )

        # Isolate and count permits at LEAN-eligible parcels
        df_group = df_group[df_group[util.LEAN_ELIGIBILITY] == util.LEAN]
        report_row[WX_COUNT_LEAN] = len( df_group )

        # Calculate LEAN weatherization subtotals per fuel type
        for s_fuel in FUELS:
            s = s_fuel.lower()
            s_lean = s + _LEAN
            report_row[s_lean] = len( df_group[df_group[util.HEATING_FUEL_DESC] == s_fuel] )

        df_summary = df_summary.append( report_row, ignore_index=True )
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

    # Initialize report column order
    summary_columns = [PERIOD, WX_COUNT]
    for s_fuel in FUELS:
        summary_columns.append( s_fuel.lower() )
    summary_columns.append( WX_COUNT_LEAN )
    for s_fuel in FUELS:
        summary_columns.append( s_fuel.lower() + _LEAN )

    # Initialize empty report dataframe
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

    # Save report to database
    util.create_table( 'WxSummaryByPeriod', conn, cur, df=df_summary )

    # Save report to excel file
    filename = 'WxSummaryByPeriod.xlsx'
    filepath = os.path.join( output_directory, filename )

    df_summary.to_excel( filepath, index=False )


######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate reports based on contents of master database' )
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

    make_wx_summary_by_period( conn, cur, engine, args.output_directory )

    util.report_elapsed_time()
