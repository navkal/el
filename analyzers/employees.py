# Copyright 2019 Energize Andover.  All rights reserved.

import argparse
import pandas as pd

import sys
sys.path.append( '../util' )
import util

#############

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Correlate employees with residents' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Read employee data and drop duplicate names
    df_employees = pd.read_sql_table( 'Employees', engine, index_col=util.ID )
    df_employees = df_employees[ [ util.LAST_NAME, util.FIRST_NAME, util.DEPARTMENT, util.POSITION ] ]
    df_employees = df_employees.drop_duplicates( subset=[util.LAST_NAME, util.FIRST_NAME] )

    # Read census data
    df_census = pd.read_sql_table( 'Census', engine, index_col=util.ID, parse_dates=True )
    df_census = df_census[ [ util.LAST_NAME, util.FIRST_NAME, util.RESIDENT_ID, util.VOTER_STATUS ] ]

    # Merge census fields Resident ID and Voter Status into employee data
    df_merge = pd.merge( df_employees, df_census, how='left', on=[util.LAST_NAME, util.FIRST_NAME] )
    sr_dups = df_merge.duplicated( subset=[util.LAST_NAME, util.FIRST_NAME], keep=False )
    df_merge.loc[ sr_dups == True, util.RESIDENT_ID ] = 'unknown'
    df_merge.loc[ sr_dups == True, util.VOTER_STATUS ] = 'unknown'
    df_merge = df_merge.drop_duplicates( subset=[util.LAST_NAME, util.FIRST_NAME] )

    # Report findings
    n_v = len( df_census[ df_census[util.VOTER_STATUS] == 'A' ] )
    n_e = len( df_merge )
    n_re = len( df_merge[ df_merge[util.RESIDENT_ID].notnull() ] )
    n_ve = len( df_merge[ df_merge[util.VOTER_STATUS] == 'A' ] )

    print( '' )
    print( 'Number of active voters: {0}'.format( n_v ) )
    print( 'Number of employees: {0}'.format( n_e ) )
    print( 'Number of resident employees: {0}'.format( n_re ) )
    print( 'Number of active voter employees: {0}'.format( n_ve ) )
    print( '' )
    print( '{0:.0f}% of town employees live in Andover.'.format( 100 * n_re / n_e ) )
    print( '{0:.0f}% of town employees are active voters in Andover.'.format( 100 * n_ve / n_e ) )
    print( 'Active voter employees represent {0:.0f}% of all Andover active voters.'.format( 100 * n_ve / n_v ) )

    print( '' )
    filename = '../analysis/employees.xlsx'
    print( 'Writing {0} rows to {1}'.format( len( df_merge ), filename ) )
    print( 'Columns: {0}'.format( list( df_merge.columns ) ) )

    # Write to spreadsheet
    df_merge.to_excel( filename, index=False )

    # Report elapsed time
    util.report_elapsed_time()
