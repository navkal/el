# Copyright 2024 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util


DEPT = util.DEPARTMENT
CPLX = util.COMPLEX
FACY = util.FACILITY
FUEL = util.FUEL
FUNT = util.FUEL_UNITS
ACCT = util.ACCOUNT_NUMBER
PROV = util.PROVIDER






#------------------------------------------------------

if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Summarize bulk electric meter data obtained from National Grid' )
    parser.add_argument( '-d', dest='db_filename', help='Database filename' )
    parser.add_argument( '-i', dest='input_table', help='Input table name' )
    parser.add_argument( '-o', dest='output_table', help='Output table name' )
    args = parser.parse_args()


    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read raw data (new format)
    df_raw = pd.read_sql_table( args.input_table, engine, index_col=util.ID )

    # Strip trailing '-dd' from dates
    df_raw[util.USAGE_END] = pd.to_datetime( df_raw[util.USAGE_END] )
    df_raw[util.USAGE_END] = df_raw[util.USAGE_END].dt.year.astype(str) + '_' + df_raw[util.USAGE_END].dt.month.astype(str).str.zfill( 2 )

    # Artificially load provider column
    df_raw[PROV] = ''
    df_raw.at[df_raw.loc[ df_raw[FUEL] == util.ELECTRIC ].index, PROV] = util.NATIONAL_GRID

    # Initialize empty dataframe that will mimic old format
    # Columns: [department, complex, facility, fuel_units, account_number, usage_end_null, one, provider, cost_or_use]
    leading_columns = list( util.CONSISTENT_COLUMN_NAMES[args.output_table].values() )
    df_old = pd.DataFrame( columns=leading_columns )

    ls_depts_debug = []

    # Replace Nan with empty string
    df_raw = df_raw.fillna( '' )

    # Iterate over raw data
    for idx, df_group in df_raw.groupby( by=[DEPT, CPLX, FACY, ACCT, FUEL, util.USAGE_END] ):

        # Extract values that identify current account
        dept = df_group.iloc[0][DEPT]
        cplx = df_group.iloc[0][CPLX]
        facy = df_group.iloc[0][FACY]
        fuel = df_group.iloc[0][FUEL]
        acct = df_group.iloc[0][ACCT]
        # print( list( df_group.iloc[0][[DEPT, CPLX, FACY, ACCT, FUEL, util.USAGE_END]].values ) )

        # Debug
        if dept not in ls_depts_debug:
            ls_depts_debug.append( dept )
            print( ' Department {}:'.format( len( ls_depts_debug ) ), dept )

        # Extract current date, to be used as a column header in the old-format dataframe
        date = df_group.iloc[0][util.USAGE_END]

        # Test whether old-format dataframe already has rows for current account
        rows_old = df_old[ ( df_old[DEPT] == dept ) & ( df_old[CPLX] == cplx ) & ( df_old[FACY] == facy ) & ( df_old[FUNT] == fuel ) & ( df_old[ACCT] == acct ) ]

        if len( rows_old ):
            # Cost and Use rows already present for this account; add column for current date
            idx = rows_old.loc[ rows_old[util.COST_OR_USE] == util.COST ].index
            df_old.at[idx, date] = df_group[util.MEI_COST].sum()
            idx = rows_old.loc[ rows_old[util.COST_OR_USE] == util.USE ].index
            df_old.at[idx, date] = df_group[util.MEI_USE].sum()

        else:
            # Rows do not exist yet for this account; initialize them and load them into old-format dataframe

            # Initialize common fields
            row_cost = pd.Series( index=leading_columns, dtype=object )
            row_cost[DEPT] = df_group.iloc[0][DEPT]
            row_cost[CPLX] = df_group.iloc[0][CPLX]
            row_cost[FACY] = df_group.iloc[0][FACY]
            row_cost[util.FUEL_UNITS] = df_group.iloc[0][FUEL]
            row_cost[ACCT] = df_group.iloc[0][ACCT]
            row_cost[PROV] = df_group.iloc[0][PROV]
            row_use = row_cost.copy()

            # Calculate Cost and Use statistics
            row_cost[util.COST_OR_USE] = util.COST
            row_cost[date] = df_group[util.MEI_COST].sum()
            row_use[util.COST_OR_USE] = util.USE
            row_use[date] = df_group[util.MEI_USE].sum()

            # Load new Cost and Use rows into old-format dataframe
            df_old = pd.concat( [df_old, pd.DataFrame( [row_cost] ), pd.DataFrame( [row_use] )] ).reset_index( drop=True )

    print( ls_depts_debug )

    # Sort data on account fields
    df_old = df_old.sort_values( by=leading_columns )

    # Fix datatype and order of numeric columns
    numeric_columns = df_old.columns[ len( leading_columns ) : ]
    df_old[numeric_columns] = df_old[numeric_columns].fillna(0).astype(int)
    df_old = df_old[leading_columns + sorted( numeric_columns ) ]

    # Create the table representing old-format raw data
    util.create_table( args.output_table, conn, cur, df=df_old )

    # Report elapsed time
    util.report_elapsed_time()
