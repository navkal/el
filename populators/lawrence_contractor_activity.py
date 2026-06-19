# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util


PARCELS_COLUMNS = \
[
    util.ACCOUNT_NUMBER,
    util.HEATING_FUEL_DESC,
    util.LEAN_ELIGIBILITY,
]

CONTRACTOR_ID = 'contractor_id'

# Set up dictionary representing columns in desired order
CONTRACTOR_COLUMNS = \
[
    util.YEAR,
    CONTRACTOR_ID,
    util.CONTRACTOR_NAME,
    util.PROJECT_TYPE,
    util.PROJECT_COUNT,
    util.TOTAL_PROJECT_COST,
]
for s_fuel in util.FUELS:
    s_fuel_prefix = s_fuel.lower() + '_'
    CONTRACTOR_COLUMNS.append( s_fuel_prefix + util.PROJECT_COUNT )
    CONTRACTOR_COLUMNS.append( s_fuel_prefix + util.PROJECT_COST )
CONTRACTOR_COLUMNS.append( util.PERMIT_REGEX )

CONTRACTOR_ROW = dict( ( el, 0 ) for el in CONTRACTOR_COLUMNS )


# Analyze contractor activity recorded in specified permit table
def analyze_contractor_activity( df, df_parcels, table_name, contractor_columns, project_type ):

    # Read specified table of building permits
    df_permits = pd.read_sql_table( table_name, engine, index_col=util.ID, parse_dates=True )

    # Merge heating fuel data from parcels table
    df_permits = pd.merge( df_permits, df_parcels, how='left', on=[util.ACCOUNT_NUMBER] )

    # Extract year from permit nmber
    df_permits[util.YEAR] = '20' + df_permits[util.PERMIT_NUMBER].str.split( '-', expand=True )[0].str[-2:]

    # Construct contractor name
    df_permits = df_permits.dropna( subset=contractor_columns, how='all' )
    df_permits[contractor_columns] = df_permits[contractor_columns].fillna( 'None' ).astype( str )
    df_permits[util.CONTRACTOR_NAME] = df_permits[contractor_columns].apply( lambda row: ' | '.join( row.values ), axis=1 )

    # Handle absence of project cost column
    if util.PROJECT_COST not in df_permits.columns:
        df_permits[util.PROJECT_COST] = 0

    # Analyze per-year activity for each contractor
    for idx, df_group in df_permits.groupby( by=[util.CONTRACTOR_NAME, util.YEAR] ):

        CONTRACTOR_ROW[util.YEAR] = df_group.iloc[0][util.YEAR]
        CONTRACTOR_ROW[util.CONTRACTOR_NAME] = df_group.iloc[0][util.CONTRACTOR_NAME]
        CONTRACTOR_ROW[util.PROJECT_TYPE] = project_type
        CONTRACTOR_ROW[util.PROJECT_COUNT] = len( df_group )
        CONTRACTOR_ROW[util.TOTAL_PROJECT_COST] = df_group[util.PROJECT_COST].sum()

        # Partition by heating fuel type
        for s_fuel in util.FUELS:
            df_fuel = df_group[df_group[util.HEATING_FUEL_DESC] == s_fuel]
            s_fuel_prefix = s_fuel.lower() + '_'
            CONTRACTOR_ROW[s_fuel_prefix + util.PROJECT_COUNT] = len( df_fuel )
            CONTRACTOR_ROW[s_fuel_prefix + util.PROJECT_COST] = df_fuel[util.PROJECT_COST].sum()

        # Partition by LEAN eligibility designations
        for s_lean in util.LEANS:
            df_lean = df_group[df_group[util.LEAN_ELIGIBILITY] == s_lean]
            s_lean_prefix = s_lean.lower() + '_'
            CONTRACTOR_ROW[s_lean_prefix + util.PROJECT_COUNT] = len( df_lean )
            CONTRACTOR_ROW[s_lean_prefix + util.PROJECT_COST] = df_lean[util.PROJECT_COST].sum()

        CONTRACTOR_ROW[util.PERMIT_REGEX] = '/' + '|'.join( list( df_group[util.PERMIT_NUMBER] ) ) + '/'

        df = df.append( CONTRACTOR_ROW, ignore_index=True )

    # Return result
    return df


##################################

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Analyze activity of weatherization and solar contractors' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Retrieve parcels table from database
    df_parcels = pd.read_sql_table( 'Assessment_L_Parcels', engine, index_col=util.ID, parse_dates=True, columns=PARCELS_COLUMNS )

    # Initialize empty dataframe
    df = pd.DataFrame( columns=CONTRACTOR_COLUMNS )

    # Analyze contractor activity recorded in specified permit tables
    df = analyze_contractor_activity( df, df_parcels, 'BuildingPermits_L_Electrical', [util.APPLICANT], 'electrical' )
    df = analyze_contractor_activity( df, df_parcels, 'BuildingPermits_L_Gas', [util.APPLICANT], 'gas' )
    df = analyze_contractor_activity( df, df_parcels, 'BuildingPermits_L_Plumbing', [util.APPLICANT], 'plumbing' )
    df = analyze_contractor_activity( df, df_parcels, 'BuildingPermits_L_Roof', [util.BUSINESS_NAME, util.APPLICANT], 'roof' )
    df = analyze_contractor_activity( df, df_parcels, 'BuildingPermits_L_Siding', [util.BUSINESS_NAME, util.APPLICANT], 'siding' )
    df = analyze_contractor_activity( df, df_parcels, 'BuildingPermits_L_Solar', [util.APPLICANT], util.SOLAR )
    df = analyze_contractor_activity( df, df_parcels, 'BuildingPermits_L_Wx', [util.BUSINESS_NAME], util.WX )

    # Load column of unique contractor IDs
    df[CONTRACTOR_ID] = df[util.CONTRACTOR_NAME].str.lower()
    df[CONTRACTOR_ID], ls_unique_values = pd.factorize( df[CONTRACTOR_ID], sort=True )
    df[CONTRACTOR_ID] = df[CONTRACTOR_ID] + 1

    # Fix numeric values
    df[util.PROJECT_COUNT] = df[util.PROJECT_COUNT].astype(int)
    df[util.TOTAL_PROJECT_COST] = df[util.TOTAL_PROJECT_COST].round().astype(int)
    for s_fuel in util.FUELS:
        s_fuel_prefix = s_fuel.lower() + '_'
        df[s_fuel_prefix + util.PROJECT_COUNT ] =  df[s_fuel_prefix + util.PROJECT_COUNT ].astype(int)
        df[s_fuel_prefix + util.PROJECT_COST ] =  df[s_fuel_prefix + util.PROJECT_COST ].round().astype(int)
    for s_lean in util.LEANS:
        s_lean_prefix = s_lean.lower() + '_'
        df[s_lean_prefix + util.PROJECT_COUNT ] =  df[s_lean_prefix + util.PROJECT_COUNT ].astype(int)
        df[s_lean_prefix + util.PROJECT_COST ] =  df[s_lean_prefix + util.PROJECT_COST ].round().astype(int)

    # Sort final result
    df = df.sort_values( by=[util.PROJECT_TYPE, util.YEAR, util.PROJECT_COUNT], ascending=[True, False, False] )

    # Save final table of commercial assessments
    util.create_table( 'ContractorActivity_L', conn, cur, df=df )

    util.report_elapsed_time()
