# Copyright 2025 Energize Andover.  All rights reserved.

######################
#
# Extract information from PDF-format electric bills in given directory
#
######################



import argparse
import os
import pdfminer.high_level as pdf_miner
import re

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import tabula


# --> Reporting of elapsed time -->
import time
START_TIME = time.time()
def report_elapsed_time( prefix='\n', start_time=START_TIME ):
    elapsed_time = round( ( time.time() - start_time ) * 1000 ) / 1000
    minutes, seconds = divmod( elapsed_time, 60 )
    ms = round( ( elapsed_time - int( elapsed_time ) ) * 1000 )
    print( prefix + 'Elapsed time: {:02d}:{:02d}.{:d}'.format( int( minutes ), int( seconds ), ms ) )
# <-- Reporting of elapsed time <--


ACCOUNT_NUMBER = 'ACCOUNT NUMBER'
SERVICE_FOR = 'SERVICE FOR'


# Get lines of text from electric bill PDF
def get_lines( filepath ):

    # Extract the text content
    pdf_text = pdf_miner.extract_text( filepath )

    # Split into lines
    ls_lines = [s.strip() for s in pdf_text.splitlines()]

    # Remove empty lines
    ls_lines = list( filter( None, ls_lines ) )

    return ls_lines


# Extract National Grid account number from bill content
def get_ng_account_number( ls_lines ):

    # Find first occurrence of label
    n_line = ls_lines.index( ACCOUNT_NUMBER )
    ls_lines = ls_lines[n_line:]

    # Find first line bearing account number format
    s_account_number = ''
    for s_line in ls_lines:
        if re.search( r'^\d{5}-\d{5}$', s_line ):
            s_account_number = s_line
            break

    # Strip hyphen
    s_account_number = s_account_number.replace( '-', '' )

    return s_account_number


# Extract National Grid service address from bill content
def get_ng_service_address( ls_lines ):

    # Initialize return value
    ls_address_lines = []

    # Find start
    n_start = -1
    for s_line in ls_lines:
        if s_line.startswith( SERVICE_FOR ):
            n_start = ls_lines.index( s_line ) + 1
            s_descr = s_line.replace( SERVICE_FOR, '' ).strip().lstrip( '(' ).rstrip( ')' )

    # Find end
    if n_start >= 0:

        for s_line in ls_lines[n_start:]:

            s_state_zip = ' '.join( s_line.split()[-2:] )

            if re.search( r'^MA \d{5}$', s_state_zip ):
                n_end = ls_lines.index( s_line ) + 1
                ls_address_lines = ls_lines[n_start:n_end]
                break

    return s_descr, ls_address_lines


# Test whether dataframe is a table of itemized charges
def is_charges_table( df ):

    b_is = False

    col = df.columns[0]
    df[col] = df[col].astype(str)

    if df[col].str.contains( r'\s' ).any():

        ls_last_word = list( df[col].str.rsplit( n=1, expand=True )[1] )

        if ( 'Charge' in ls_last_word ) or ( 'Chg' in ls_last_word ):
            b_is = True

    return b_is


# Test whether string represents a numeric value
def is_number( s ):
    try:
        float( s )
        return True
    except ValueError:
        return False


# Extract charges in dataframe to dictionary
def df_to_dc_charges( df ):

    col_1 = df.columns[0]
    col_n = df.columns[-1]

    dc_charges = {}

    for index, row in df.iterrows():

        # Extract label from first column, value from last column
        s_label = row[col_1]
        s_value = row[col_n]

        if ( s_label.endswith( ' Charge' ) or s_label.endswith( ' Chg' ) ) and is_number( s_value ):
            s_key = s_label.lower().replace( ' ', '_' )
            dc_charges[s_key] = float( s_value )

    return dc_charges


# Extract itemized charges from bill content
def get_itemized_charges( filepath ):

    # Initialize empty dictionary of itemized charges
    dc_charges = {}

    try:
        # Extract tables from the bill
        ls_dfs = tabula.read_pdf( filepath, pages='all', multiple_tables=True, lattice=False, )

        # Iterate over dataframes
        for df in ls_dfs:
            df = df.copy()

            # Determine whether this dataframe represents table of charges
            b_is = is_charges_table( df )

            if not b_is:
                # Drop first column and retry
                df = df[df.columns[1:]]
                b_is = is_charges_table( df )

            if b_is:
                dc_charges = df_to_dc_charges( df )

    except Exception as e:
        pass

    return dc_charges


###############################


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Extract data from PDF-format electric bills' )
    parser.add_argument( '-i', dest='input_directory', default=os.getcwd(), help='Input directory containing electric bills' )
    parser.add_argument( '-o', dest='output_filename', default=os.getcwd(), help='Full path to output CSV file' )
    args = parser.parse_args()

    print( '' )
    print( f'Reading electric bills from: {args.input_directory}' )
    print( f'Saving bill attributes to: {args.output_filename}' )
    print( '' )

    ls_bills = []

    ls_trouble = []

    n_file = 0

    for filename in os.listdir( args.input_directory ):

        if filename.lower().endswith( '.pdf' ):

            n_file += 1

            # Find the file
            filepath = os.path.join( args.input_directory, filename )

            # Extract lines of text
            ls_lines = get_lines( filepath )

            # Extract National Grid account number
            s_account_number = get_ng_account_number( ls_lines )

            # Extract service address
            s_descr, ls_address_lines = get_ng_service_address( ls_lines )

            # Extract itemized charges
            dc_charges = get_itemized_charges( filepath )

            # Report extracted account number
            s_report = f'{n_file} - {filename}: {s_account_number}'
            if s_account_number != filename[:10]:
                s_report += ' - TROUBLE!'
                ls_trouble.append( { filename: s_account_number } )
            print( s_report )

            # Report extracted service address
            s_report = f'Description: <{s_descr}>, Address: {ls_address_lines}'
            if not( ls_address_lines and re.search( r'^\d{5}$', ls_address_lines[-1].split()[-1] ) ):
                s_report += ' - TROUBLE!'
                ls_trouble.append( { filename: s_account_number } )
            print( s_report )

            # Construct dictionary from bill attributes
            dc_bill = \
            {
                'account_number': s_account_number,
                'description': s_descr,
                'address_line_1': None,
                'address_line_2': None,
                'address_line_3': None,
                'address_line_4': None,
                'address_line_5': None,
                'address_line_6': None,
                'address_line_7': None,
                'address_line_8': None,
                'address_line_9': None,
                'city_state_zip': ls_address_lines[-1]
            }

            ls_address_lines = ls_address_lines[:-1]
            n_address_line = 0
            for s_line in ls_address_lines:
                n_address_line += 1
                dc_bill['address_line_' + str( n_address_line )] = s_line

            for s_key in dc_charges.keys():
                dc_bill[s_key] = dc_charges[s_key]

            # Append dictionary to list
            ls_bills.append( dc_bill )


    # Construct dataframe from list of bills
    df_bills = pd.DataFrame( ls_bills )
    
    # Clean up
    df_bills = df_bills.dropna( axis='columns', how='all' )
    df_bills = df_bills.sort_values( by=['account_number'] )
    
    # Save to CSV
    df_bills.to_csv( args.output_filename, index=False )

    # Report trouble
    if len( ls_trouble ):
        print( '' )
        print( 'Possible trouble:' )
        for dc_trouble in ls_trouble:
            print( dc_trouble )

    # Report elapsed time
    report_elapsed_time()
