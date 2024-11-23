# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util


COLUMNS = \
[
    util.YEAR,
    util.CONTRACTOR_NAME,
    util.PROJECT_TYPE,
    util.PROJECT_COUNT,
    util.TOTAL_PROJECT_COST,
    util.PERMIT_REGEX,
]

ROW = dict( ( el, 0 ) for el in COLUMNS )


# Analyze contractor activity recorded in specified permit table
def analyze_contractor_activity( df, table_name, contractor_columns, project_type ):

    # Read specified table of building permits
    df_permits = pd.read_sql_table( table_name, engine, index_col=util.ID, parse_dates=True )

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

        ROW[util.YEAR] = df_group.iloc[0][util.YEAR]
        ROW[util.CONTRACTOR_NAME] = df_group.iloc[0][util.CONTRACTOR_NAME]
        ROW[util.PROJECT_TYPE] = project_type
        ROW[util.PROJECT_COUNT] = len( df_group )
        ROW[util.TOTAL_PROJECT_COST] = df_group[util.PROJECT_COST].sum()
        ROW[util.PERMIT_REGEX] = '/' + '|'.join( list( df_group[util.PERMIT_NUMBER] ) ) + '/'

        df = df.append( ROW, ignore_index=True )

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

    # Initialize empty dataframe
    df = pd.DataFrame( columns=COLUMNS )

    # Analyze contractor activity recorded in specified permit tables
    df = analyze_contractor_activity( df, 'BuildingPermits_L_Electrical', [util.APPLICANT], 'electrical' )
    df = analyze_contractor_activity( df, 'BuildingPermits_L_Gas', [util.APPLICANT], 'gas' )
    df = analyze_contractor_activity( df, 'BuildingPermits_L_Plumbing', [util.APPLICANT], 'plumbing' )
    df = analyze_contractor_activity( df, 'BuildingPermits_L_Roof', [util.BUSINESS_NAME, util.APPLICANT], 'roof' )
    df = analyze_contractor_activity( df, 'BuildingPermits_L_Siding', [util.BUSINESS_NAME, util.APPLICANT], 'siding' )
    df = analyze_contractor_activity( df, 'BuildingPermits_L_Solar', [util.APPLICANT], util.SOLAR )
    df = analyze_contractor_activity( df, 'BuildingPermits_L_Wx', [util.BUSINESS_NAME], util.WX )
    df[util.TOTAL_PROJECT_COST] = df[util.TOTAL_PROJECT_COST].round().astype(int)
    df[util.PROJECT_COUNT] = df[util.PROJECT_COUNT].astype(int)

    # Sort final result
    df = df.sort_values( by=[util.PROJECT_TYPE, util.YEAR, util.PROJECT_COUNT], ascending=[True, False, False] )

    # Save final table of commercial assessments
    util.create_table( 'ContractorActivity_L', conn, cur, df=df )

    util.report_elapsed_time()
