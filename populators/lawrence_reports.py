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

# Generate report on weatherization permit counts
def make_wx_count_report( conn, cur, engine, output_directory ):

    # Read extended table of weatherization permits from database
    df = pd.read_sql_table( 'BuildingPermits_L_Wx_Extended', engine, index_col=util.ID, parse_dates=True )

    # Isolate and sort usable rows
    df = df.dropna( subset=[util.DATE_ISSUED] )
    df = df[df[util.HEATING_FUEL_DESC].isin( FUELS )]
    df = df.sort_values( by=[util.DATE_ISSUED] )

    # Add period column indicating year and quarter
    df[Y] = df[util.DATE_ISSUED].str.split( '-', expand=True )[0]
    df[DT] = pd.to_datetime( df[util.DATE_ISSUED] )
    df[Q] = df[DT].dt.quarter.astype(str)
    df[PERIOD] = df[Y] + ' Q' + df[Q]

    # Initialize report column order
    report_columns = [PERIOD, WX_COUNT]
    for s_fuel in FUELS:
        report_columns.append( s_fuel.lower() )
    report_columns.append( WX_COUNT_LEAN )
    for s_fuel in FUELS:
        report_columns.append( s_fuel.lower() + _LEAN )

    # Initialize empty report dataframe
    df_report = pd.DataFrame( columns=report_columns )

    # Build next row and append to report dataframe
    for idx, df_group in df.groupby( by=[PERIOD] ):

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

        df_report = df_report.append( report_row, ignore_index=True )

    # Fix numeric types
    for s_fuel in FUELS:
        s = s_fuel.lower()
        s_lean = s + _LEAN
        df_report[s] = df_report[s].astype( int )
        df_report[s_lean] = df_report[s_lean].astype( int )

    # Save report to database
    util.create_table( 'BuildingPermits_L_Wx_Report', conn, cur, df=df_report )

    # Save report to excel file
    filename = 'BuildingPermits_L_Wx_Report.xlsx'
    filepath = os.path.join( output_directory, filename )

    df_report.to_excel( filepath, index=False )


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

    make_wx_count_report( conn, cur, engine, args.output_directory )

    util.report_elapsed_time()
