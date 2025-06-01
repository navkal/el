# Copyright 2025 Energize Andover.  All rights reserved.

######################
#
# Extract information from collection of PDF-format electric bills
#
# Optional parameters:
# -i <input directory> - defaults to current working directory
#
# Required parameters:
# -o <output CSV file path>
#
# Sample parameter sequences:
#
#  -i ./in/electric_bills -o ./out/electric_bills.csv
#
######################

LS_TROUBLE_ACCOUNTS = \
[
]

B_DEBUG = False
N_DEBUG = 20
if B_DEBUG:
    print( '' )
    print( '---------------------' )
    print( 'Running in debug mode' )
    print( '---------------------' )
    print( '' )


import argparse
import os

import pdfminer.high_level as pdf_miner
import pdfplumber

import re

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )



# --> Reporting of elapsed time -->
import time
START_TIME = time.time()
def report_elapsed_time( prefix='\n', start_time=START_TIME ):
    elapsed_time = round( ( time.time() - start_time ) * 1000 ) / 1000
    minutes, seconds = divmod( elapsed_time, 60 )
    ms = round( ( elapsed_time - int( elapsed_time ) ) * 1000 )
    print( prefix + 'Elapsed time: {:02d}:{:02d}.{:d}'.format( int( minutes ), int( seconds ), ms ) )
# <-- Reporting of elapsed time <--


# Format regular expression as a capture group
def capture( s ):
    return '(' + s + ')'


# Print in debug mode
def debug_print( s ):
    if B_DEBUG:
        print( s )


# Track bills missing customer charge or line item
LS_CC_NOT_FOUND = []
LS_LI_NOT_FOUND = []


# Literal text that appears in bills
LBL_ACCOUNT_NUMBER = 'ACCOUNT NUMBER'
LBL_SERVICE_FOR = 'SERVICE FOR'
LBL_CUSTOMER_CHARGE = 'Customer Charge'


# Mappings of words that appear in labels
LABEL_WORDS = \
{
    'Charge': 'Chg',
    'Pk': 'Peak',
    'Distribution': 'Dist',
}


# Regular expressions
RE_TOTAL_ENERGY = 'Total Energy (.*) kWh'
RE_METER_NUMBER = 'METER NUMBER (.*) NEXT SCHEDULED READ DATE ON OR ABOUT'
RE_RATE_VOLTAGE = 'RATE (.*) VOLTAGE DELIVERY LEVEL (.*)'

RE_LABEL = '[A-Z][a-z]+(?: [A-Z][a-z]+)*'
RE_NUMBER = '-?(?:\d+(?:\.\d+)?|\.\d+)'
RE_UNIT = '[a-zA-Z]+(?:/[a-zA-Z]+)?'
RE_SPACES = ' +'
RE_WHITESPACE = '\s+'

RE_CUSTOMER_CHARGE = capture( LBL_CUSTOMER_CHARGE ) + RE_WHITESPACE + capture( RE_NUMBER ) + RE_WHITESPACE
RE_LINE_ITEM = capture( RE_LABEL ) + RE_SPACES + capture( RE_NUMBER ) + RE_SPACES + 'x' + RE_SPACES + capture( RE_NUMBER ) + capture( RE_UNIT ) + RE_SPACES + capture( RE_NUMBER )

# Label suffixes
_DLRS = '_$'
_RATE = '_rate' + _DLRS
_USED = '_used'


# Dataframe column names
ACCOUNT_NUMBER = 'account_number'
DESCRIPTION = 'description'
ADDRESS_LINE_ = 'address_line_'
CITY_STATE_ZIP = 'city_state_zip'
TOTAL_ENERGY = 'total_energy'
METER_NUMBER = 'meter_number'
RATE_CLASS = 'rate_class'
VOLTAGE_LEVEL = 'voltage_level'
KWH_USED = 'kwh' + _USED
KW_USED = 'kw' + _USED
CUSTOMER_CHG = 'customer_chg_$'

# Dataframe leading columns
LEADING_COLUMNS = \
{
    ACCOUNT_NUMBER: None,
    DESCRIPTION: None,
    ADDRESS_LINE_ + '1': None,
    ADDRESS_LINE_ + '2': None,
    ADDRESS_LINE_ + '3': None,
    ADDRESS_LINE_ + '4': None,
    ADDRESS_LINE_ + '5': None,
    ADDRESS_LINE_ + '6': None,
    ADDRESS_LINE_ + '7': None,
    ADDRESS_LINE_ + '8': None,
    ADDRESS_LINE_ + '9': None,
    CITY_STATE_ZIP: None,
    TOTAL_ENERGY: None,
    METER_NUMBER: None,
    RATE_CLASS: None,
    VOLTAGE_LEVEL: None,
    KWH_USED: None,
    KW_USED: None,
    CUSTOMER_CHG: None,
}


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
    n_line = ls_lines.index( LBL_ACCOUNT_NUMBER )
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
        if s_line.startswith( LBL_SERVICE_FOR ):
            n_start = ls_lines.index( s_line ) + 1
            s_descr = s_line.replace( LBL_SERVICE_FOR, '' ).strip().lstrip( '(' ).rstrip( ')' )

    # Find end
    if n_start >= 0:

        for s_line in ls_lines[n_start:]:

            s_state_zip = ' '.join( s_line.split()[-2:] )

            if re.search( r'^MA \d{5}$', s_state_zip ):
                n_end = ls_lines.index( s_line ) + 1
                ls_address_lines = ls_lines[n_start:n_end]
                break

    return s_descr, ls_address_lines


# Extract fields from Delivery Services section of bill
def get_delivery_services( filepath ):

    dc_ds = {}

    # Open the file
    with pdfplumber.open( filepath ) as pdf:

        # Iterate over PDF pages
        for page in pdf.pages:

            text = page.extract_text()
            # Extract customer charge
            matches = re.findall( RE_TOTAL_ENERGY, text )
            for m in matches:
                dc_ds[TOTAL_ENERGY] = m[0]

            # Extract customer charge
            matches = re.findall( RE_METER_NUMBER, text )
            for m in matches:
                dc_ds[METER_NUMBER] = m[0]

            # Extract customer charge
            matches = re.findall( RE_RATE_VOLTAGE, text )
            for m in matches:
                dc_ds[RATE_CLASS] = m[0]
                dc_ds[VOLTAGE_LEVEL] = m[1]

    debug_print( dc_ds )

    return dc_ds



# Generate column name from label string
def make_column_name( s_label ):

    # Normalize label words that have inconsistent spellings
    for s_word in LABEL_WORDS.keys():
        s_label = s_label.replace( s_word, LABEL_WORDS[s_word] )

    # Change case and delimiters
    s_column_name = s_label.lower().replace( ' ', '_' )

    return s_column_name


# Save matches in dictionary
def matches_to_dc_charges( matches, dc_charges ):

    for m in matches:

        # First match is label; make it into a column name
        s_column_name = make_column_name( m[0] )

        # Middle matches, if present, are rate, usage, and unit
        if len( m ) >= 5:

            # Incorporate unit into column name
            s_unit = m[3].lower()
            s_column_name += '_' + s_unit

            # Save rate
            dc_charges[s_column_name + _RATE] = float( m[1] )

            # Save usage applicable to current unit
            dc_charges[s_unit + _USED] = float( m[2] )

        # Last match is dollar amount charged
        dc_charges[s_column_name + _DLRS] = float( m[-1] )

    return dc_charges


# Extract charges from bill content
def get_charges( filepath ):

    # Initialize empty dictionary of charges
    dc_charges = {}

    b_got_cc = False
    b_got_li = False

    # Open the file
    with pdfplumber.open( filepath ) as pdf:

        # Iterate over PDF pages
        for page in pdf.pages:

            text = page.extract_text()

            # Extract customer charge
            matches = re.findall( RE_CUSTOMER_CHARGE, text )
            if matches:
                b_got_cc = True
                matches_to_dc_charges( matches, dc_charges )

            # Extract line iterms
            matches = re.findall( RE_LINE_ITEM, text )
            if matches:
                b_got_li = True
                matches_to_dc_charges( matches, dc_charges )

    # Track whether this bill contains Customer Charge
    if not b_got_cc:
        LS_CC_NOT_FOUND.append( { filename: s_account_number } )

    # Track whether this bill contains line items
    if not b_got_li:
        LS_LI_NOT_FOUND.append( { filename: s_account_number } )

    return dc_charges


# Construct dataframe from list of bills
def make_df_bills( ls_bills ):

    df = pd.DataFrame( ls_bills )

    # Sort trailing columns alphabetically
    n_leading = len( LEADING_COLUMNS )
    ls_cols = list( df.columns )
    n_cols = len( ls_cols )
    n_start = n_leading - n_cols

    ls_trailing = ls_cols[n_start:]
    ls_trailing_sorted = sorted( ls_trailing )
    ls_leading = list( df.columns[:n_start] )
    ls_reordered = ls_leading + ls_trailing_sorted

    df = df.reindex( columns=ls_reordered )

    # Drop empty columns
    df = df.dropna( axis='columns', how='all' )

    # Sort dataframe on account number
    df = df.sort_values( by=[ACCOUNT_NUMBER] )

    return df


###############################


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Extract data from PDF-format electric bills' )
    parser.add_argument( '-i', dest='input_directory', default=os.getcwd(), help='Input directory containing electric bills' )
    parser.add_argument( '-o', dest='output_filename', help='Full path to output CSV file', required=True )
    args = parser.parse_args()

    print( '' )
    print( f'Reading electric bills from: {args.input_directory}' )
    print( f'Saving bill attributes to: {args.output_filename}' )
    print( '' )

    # Report start time
    print( 'Starting at', time.strftime( '%H:%M:%S', time.localtime( START_TIME ) ) )
    print( '' )

    ls_bills = []

    ls_trouble = []

    n_file = 0

    for filename in os.listdir( args.input_directory ):

        if filename.lower().endswith( '.pdf' ):

            n_file += 1

            # Debug mode: quit early
            if B_DEBUG and ( n_file > N_DEBUG ):
                break

            # Find the file
            filepath = os.path.join( args.input_directory, filename )

            # Extract lines of text
            ls_lines = get_lines( filepath )

            # Extract National Grid account number
            s_account_number = get_ng_account_number( ls_lines )

            # Report extracted account number
            s_report = f'{n_file} - {filename}: {s_account_number}'
            if s_account_number != filename[:10]:
                s_report += ' - TROUBLE!'
                ls_trouble.append( { filename: s_account_number } )
            print( s_report )

            # Extract service address
            s_descr, ls_address_lines = get_ng_service_address( ls_lines )

            # Report extracted service address
            s_report = f'Description: <{s_descr}>, Address: {ls_address_lines}'
            if not( ls_address_lines and re.search( r'^\d{5}$', ls_address_lines[-1].split()[-1] ) ):
                s_report += ' - TROUBLE!'
                ls_trouble.append( { filename: s_account_number } )
            print( s_report )

            # Extract fields from Delivery Services section
            dc_ds = get_delivery_services( filepath )

            # Extract charges from bill
            dc_charges = get_charges( filepath )

            # Construct dictionary from bill attributes
            dc_bill = LEADING_COLUMNS.copy()
            dc_bill[ACCOUNT_NUMBER] = s_account_number
            dc_bill[DESCRIPTION] = s_descr
            dc_bill[CITY_STATE_ZIP] = ls_address_lines[-1]

            ls_address_lines = ls_address_lines[:-1]
            n_address_line = 0
            for s_line in ls_address_lines:
                n_address_line += 1
                dc_bill[ADDRESS_LINE_ + str( n_address_line )] = s_line

            # Add fields from Delivery Services section
            for s_key in dc_ds.keys():
                dc_bill[s_key] = dc_ds[s_key]

            # Add itemized charges
            for s_key in dc_charges.keys():
                dc_bill[s_key] = dc_charges[s_key]

            # Append dictionary to list
            ls_bills.append( dc_bill )


    # Construct dataframe from list of bills
    df_bills = make_df_bills( ls_bills )

    # Save to CSV
    df_bills.to_csv( args.output_filename, index=False )

    # Report trouble
    if len( ls_trouble ):
        print( '' )
        print( 'Possible trouble:' )
        for dc_trouble in ls_trouble:
            print( dc_trouble )

    # Report Customer Charge not found
    if len( LS_CC_NOT_FOUND ):
        print( '' )
        print( 'Customer Charge not found:' )
        n = 0
        for dc in LS_CC_NOT_FOUND:
            n += 1
            print( f' {n}: {dc}' )

    # Report Line Items not found
    if len( LS_LI_NOT_FOUND ):
        print( '' )
        print( 'Line Items not found:' )
        n = 0
        for dc in LS_LI_NOT_FOUND:
            n += 1
            print( f' {n}: {dc}' )

    # Report elapsed time
    report_elapsed_time()
