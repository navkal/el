# Copyright 2022 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option('display.max_rows', 500)
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

#------------------------------------------------------

HISTORY = 'history'
ORIGINAL = 'Original'
RETAINED = 'Retained'
DROPPED = 'Dropped'
UPDATED = 'Updated'

# Handle conflicts between rows that represent same account
def clean_conflicts( df ):

    # Find rows that bear conflicting information about a specific account
    sr_b_duped = df.duplicated( subset=[util.ACCOUNT_NUMBER, util.COST_OR_USE], keep=False )
    df_dupes = df.iloc[ sr_b_duped[sr_b_duped].index.values ]

    # Initialize dataframe representing history of conflict removal
    df_history = df_dupes.copy()
    df_history[HISTORY] = ORIGINAL
    df_history.insert( 0, HISTORY, df_history.pop( HISTORY ) )

    # Initialize list of conflicting rows to drop
    ls_drop_idx = []

    # Iterate over groups of conflicting rows
    for idx_group, df_group in df_dupes.groupby( by=[util.ACCOUNT_NUMBER, util.COST_OR_USE] ):

        # Initialize dictionary that will indicate which is the preferred row among the conflicts
        dc_preference_score = {}

        # Iterate over current group of conflicting rows, calculating preference score for each
        for idx_row, row in df_group.iterrows():

            # Calculate preference score for current row.  (Low is good.)
            n_preference_score = 0
            n_weight = 1
            for col_name in df_group.columns:
                if is_tally_column( col_name ) and row[col_name] == 0:
                    n_preference_score += n_weight
                    n_weight = n_weight + 1

            # Save preference score for current row
            dc_preference_score[idx_row] = n_preference_score

        # Get index of preferred row, i.e., row with minimum preference score
        idx_preferred = min( dc_preference_score, key=dc_preference_score.get )

        # Add remaining, non-preferred rows to list of rows to be dropped
        del dc_preference_score[idx_preferred]
        ls_drop_idx.extend( list( dc_preference_score.keys() ) )

        # Optionally update preferred row with values from other rows
        b_preferred_row_updated = False
        for col_name in df_group.columns:

            # If this is a tally column...
            if is_tally_column( col_name ):

                # ... and current value is not already the maximum
                cur_value = df[col_name].values[idx_preferred]
                max_value = df_group[col_name].max()
                if pd.notnull( max_value ) and ( cur_value != max_value ):

                    # Update the cell
                    df.at[idx_preferred, col_name] = max_value
                    b_preferred_row_updated = True

        # Insert updated row into history
        if b_preferred_row_updated:
            df_history = df_history.append( df.loc[idx_preferred] )
        else:
            df_history.loc[idx_preferred, HISTORY] = RETAINED

    # Drop non-preferred conflicting rows and finalize history
    if len( ls_drop_idx ):
        df = df.drop( index=ls_drop_idx )
        df = df.reset_index( drop=True )

        df_history.loc[ls_drop_idx, HISTORY] = DROPPED
        df_history[HISTORY] = df_history[HISTORY].fillna( UPDATED )
        df_history = df_history.sort_values( by=[util.ACCOUNT_NUMBER, util.COST_OR_USE, HISTORY] )
        df_history = df_history.reset_index( drop=True )

    return df, df_history


def clean_external_suppliers_electric( df_raw_el, df_mei ):
    df_clean_el = df_raw_el.copy()
    df_clean_el[util.ACCOUNT_NUMBER] = df_clean_el[util.ACCOUNT_NUMBER].astype(str)
    df_clean_el = pd.merge( df_clean_el, df_mei, how='left', on=[util.ACCOUNT_NUMBER] )
    df_clean_el[util.MEI_TRACKING] = df_clean_el[util.MEI_TRACKING].fillna( util.NO )
    df_clean_el.insert( df_clean_el.columns.get_loc( util.ACCOUNT_NUMBER ) + 1, util.MEI_TRACKING, df_clean_el.pop( util.MEI_TRACKING ) )
    return df_clean_el


def clean_external_suppliers_gas( df_raw_gas, df_mei ):
    df_clean_gas = df_raw_gas.copy()
    df_clean_gas[util.ACCOUNT_NUMBER] = df_clean_gas[util.UTILITY_ACCOUNT].str.split( '-', expand=True )[0]
    df_clean_gas = pd.merge( df_clean_gas, df_mei, how='left', on=[util.ACCOUNT_NUMBER] )
    df_clean_gas[util.MEI_TRACKING] = df_clean_gas[util.MEI_TRACKING].fillna( util.NO )
    df_clean_gas.insert( df_clean_gas.columns.get_loc( util.UTILITY_ACCOUNT ) + 1, util.ACCOUNT_NUMBER, df_clean_gas.pop( util.ACCOUNT_NUMBER ) )
    df_clean_gas.insert( df_clean_gas.columns.get_loc( util.ACCOUNT_NUMBER ) + 1, util.MEI_TRACKING, df_clean_gas.pop( util.MEI_TRACKING ) )
    return df_clean_gas


# Strip leading zeros from numeric values
def strip_leading_zeros( sr ):
    df_result = sr.to_frame( name='original' )
    df_result['numeric'] = sr[sr.str.isdecimal()]
    df_result['not_numeric'] = sr[~sr.str.isdecimal()]
    df_result['stripped'] = df_result['numeric'].str.replace( r'^0+(?!.*\D)', '', regex=True )
    df_result = df_result.fillna( '' )
    df_result['result'] = df_result['stripped'] + df_result['not_numeric']
    return df_result['result']


# Names of columns representing monthly tallies start with 4-digit year.
def is_tally_column( col_name ):
    return col_name[:4].isnumeric()



if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Clean up Mass Energy Insight and optional related tables' )
    parser.add_argument( '-d', dest='db_filename', help='Database filename' )
    parser.add_argument( '-i', dest='input_table', help='Input table name' )
    parser.add_argument( '-z', dest='input_table_iso_zones', help='Input table name: mapping from account numbers to ISO zones' )
    parser.add_argument( '-e', dest='input_table_electric', help='Input table name: external electricity suppliers' )
    parser.add_argument( '-g', dest='input_table_gas', help='Input table name: external gas suppliers' )
    parser.add_argument( '-o', dest='output_table', help='Output table name' )
    parser.add_argument( '-p', dest='output_table_electric', help='Output table name: external electricity suppliers' )
    parser.add_argument( '-q', dest='output_table_gas', help='Output table name: external gas suppliers' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read raw input table from database
    df_raw = pd.read_sql_table( args.input_table, engine, index_col=util.ID )
    df = df_raw.copy()
    df[util.ACCOUNT_NUMBER] = strip_leading_zeros( df[util.ACCOUNT_NUMBER] )

    # Read and merge optional ISO zone information
    if args.input_table_iso_zones is not None:
        df_iso_zones = pd.read_sql_table( args.input_table_iso_zones, engine, index_col=util.ID )
        df_iso_zones[util.ACCOUNT_NUMBER] = df_iso_zones[util.ACCOUNT_NUMBER].astype( str )
        df = pd.merge( df, df_iso_zones, how='left', on=[util.ACCOUNT_NUMBER] )
        df.insert( df.columns.get_loc( util.ACCOUNT_NUMBER ) + 1, util.ISO_ZONE, df.pop( util.ISO_ZONE ) )

    # Read optional table of external electricity suppliers from database
    if args.input_table_electric is not None:
        df_raw_el = pd.read_sql_table( args.input_table_electric, engine, index_col=util.ID )
        df_el = df_raw_el[ [util.ACCOUNT_NUMBER, util.LOCATION_ADDRESS] ].copy()
    else:
        df_raw_el = df_el = pd.DataFrame( columns=[util.ACCOUNT_NUMBER, util.LOCATION_ADDRESS] )

    df_es = df_el.copy()

    # Read optional table of external gas suppliers from database
    if args.input_table_gas is not None:

        # Read the table
        df_raw_gas = pd.read_sql_table( args.input_table_gas, engine, index_col=util.ID )
        df_gas = df_raw_gas[ [util.UTILITY_ACCOUNT, util.STREET, util.CITY, util.STATE, util.ZIP] ].copy()

        # Extract account number
        df_gas[util.ACCOUNT_NUMBER] = df_gas[util.UTILITY_ACCOUNT].str.split( '-', expand=True )[0]

        # Concatenate address fragments into single field
        df_gas[util.LOCATION_ADDRESS] = df_gas[util.STREET] + ' ' + df_gas[util.CITY] + ' ' + df_gas[util.STATE] + ' ' + df_gas[util.ZIP]
        df_gas = df_gas[ [util.ACCOUNT_NUMBER, util.LOCATION_ADDRESS] ]
    else:
        df_raw_gas = df_gas = pd.DataFrame( columns=[util.ACCOUNT_NUMBER, util.LOCATION_ADDRESS] )

    # Combine all external suppliers into single dataframe
    df_es = df_es.append( df_gas, ignore_index=True )
    df_es[util.ACCOUNT_NUMBER] = df_es[util.ACCOUNT_NUMBER].astype(str)
    df_es = df_es.drop_duplicates()

    # Merge optional external supplier flag into dataframe
    if len( df_es ):
        df_es[util.EXTERNAL_SUPPLIER] = util.YES
        df = pd.merge( df, df_es, how='left', on=[util.ACCOUNT_NUMBER] )
        df[util.EXTERNAL_SUPPLIER] = df[util.EXTERNAL_SUPPLIER].fillna( util.NO )

        # Reorder columns
        df.insert( df.columns.get_loc( util.ACCOUNT_NUMBER ) + 1, util.EXTERNAL_SUPPLIER, df.pop( util.EXTERNAL_SUPPLIER ) )
        df.insert( df.columns.get_loc( util.EXTERNAL_SUPPLIER ) + 1, util.LOCATION_ADDRESS, df.pop( util.LOCATION_ADDRESS ) )

    # Convert tallies from text to numeric.
    for col_name in df.columns:
        if is_tally_column( col_name ) and ( df[col_name].dtype == object ):

            # Clean up text
            df[col_name] = df[col_name].replace( ',', '', regex=True )

            # Convert to numeric datatype
            df[col_name] = df[col_name].fillna( 0 ).astype( int )

    # Handle conflicts between rows representing same account
    df, df_history = clean_conflicts( df )

    # Save result to database
    util.create_table( args.output_table, conn, cur, df=df )

    # Optionally save change history to database
    if len( df_history ):
        util.create_table( args.output_table + '_ChangeHistory', conn, cur, df=df_history )

    # Optionally clean and save external supplier tables

    if args.output_table_electric is not None or args.output_table_gas is not None:
        df_mei = df.copy()
        df_mei = df_mei[ [util.ACCOUNT_NUMBER] ]
        df_mei[util.MEI_TRACKING] = util.YES
        df_mei = df_mei.drop_duplicates()

        if args.output_table_electric is not None:
            util.create_table( args.output_table_electric, conn, cur, df=clean_external_suppliers_electric( df_raw_el, df_mei ) )

        if args.output_table_gas is not None:
            util.create_table( args.output_table_gas, conn, cur, df=clean_external_suppliers_gas( df_raw_gas, df_mei ) )

    # Report elapsed time
    util.report_elapsed_time()
