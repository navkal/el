# Copyright 2025 Energize Lawrence.  All rights reserved.

######################
#
# Extract information from collection of PDF-format electric bills
#
# Optional input directory:
# -i <input directory> - defaults to current working directory
#
# Supply at least one output file path:
# -c <output CSV file path>
# -x <output Excel file path>
# -s <output SQLite file path>
#
# Applicable only with -s SQLite output file:
# -t <output SQLite table name> - defaults to 'ElectricBills'
#
# Optional flag to number columns:
# -n <number columns?> - defaults to False
#
# Sample parameter sequence:
#
# -i ./in/electric_bills -c ./out/electric_bills.csv
#
######################

B_DEBUG = False
# B_DEBUG = True
N_DEBUG = 20
if B_DEBUG:
    print( '' )
    print( '---------------------' )
    print( 'Running in debug mode' )
    print( '---------------------' )
    print( '' )


import argparse
import os
import shutil

import pdfminer.high_level as pdf_miner
import pdfplumber
import fitz  # PyMuPDF

import re

from datetime import datetime

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sqlite3


# --> Reporting of elapsed time -->
import time
START_TIME = time.time()
def report_elapsed_time( prefix='\n', start_time=START_TIME ):
    elapsed_time = round( ( time.time() - start_time ) * 1000 ) / 1000
    minutes, seconds = divmod( elapsed_time, 60 )
    ms = round( ( elapsed_time - int( elapsed_time ) ) * 1000 )
    print( prefix + 'Elapsed time: {:02d}:{:02d}.{:d}'.format( int( minutes ), int( seconds ), ms ) )
# <-- Reporting of elapsed time <--


# Temporary directory for caching split bills
TEMP_DIR_NAME = 'DELETE_THESE_SPLIT_BILLS'


# Format regular expression as a capture group
def capture( s ):
    return '(' + s + ')'


# Convert numeric string with commas to float
def comma_float( s_num ):
    return float( s_num.replace( ',', '' ) )


# Print in debug mode
def debug_print( s ):
    if B_DEBUG:
        print( s )


# Literal text that appears in bills
LBL_CUSTOMER_SERVICE = 'CUSTOMER SERVICE'
LBL_ACCOUNT_NUMBER = 'ACCOUNT NUMBER'
LBL_SERVICE_FOR = 'SERVICE FOR'
LBL_DATE_BILL_ISSUED = 'DATE BILL ISSUED'
LBL_BILLING_PERIOD = 'BILLING PERIOD'
LBL_CURRENT_CHARGES = 'Current Charges'
LBL_CUSTOMER_CHARGE = 'Customer Charge'
LBL_LATE_PAYMENT_CHARGE = 'Late Payment Charges'
LBL_TRANSFER_CREDIT = 'Transfer of Remote Net Meter Credit'

# Mappings of words that appear in labels
LABEL_WORDS = \
{
    'Charges': 'Chg',
    'Charge': 'Chg',
    'Pk': 'Peak',
    'Distribution': 'Dist',
}


# Regular expressions
RE_TOTAL_ENERGY = 'Total Energy (.*) kWh'
RE_METER_NUMBER = 'METER NUMBER (.*) NEXT SCHEDULED READ DATE ON OR ABOUT'
RE_RATE_VOLTAGE = 'RATE (.*) VOLTAGE DELIVERY LEVEL (.*)'
RE_SUPPLIER = 'SUPPLIER (\w+(?: \w+)*)'
RE_LOADZONE = 'Loadzone ([A-Z/]+)'

RE_LABEL = '[A-Z][a-z]+(?: [A-Z][a-z]+)*'
RE_NUMBER = '-?\d{1,3}(?:,\d{3})*(?:\.\d+)?|-?\d+(?:\.\d+)?'
RE_UNIT = '[a-zA-Z]+(?:/[a-zA-Z]+)?'
RE_SPACES = ' +'
RE_WHITESPACE = '\s+'
RE_OPTIONAL_SPACE = ' *'

RE_CURRENT_CHARGES = capture( LBL_CURRENT_CHARGES ) + RE_WHITESPACE + capture( RE_NUMBER ) + RE_WHITESPACE + capture( RE_NUMBER ) + RE_WHITESPACE + capture( RE_NUMBER ) + RE_WHITESPACE + capture( RE_NUMBER )
RE_CUSTOMER_CHARGE = capture( LBL_CUSTOMER_CHARGE ) + RE_WHITESPACE + capture( RE_NUMBER ) + RE_WHITESPACE
RE_LATE_PAYMENT_CHARGE = capture( LBL_LATE_PAYMENT_CHARGE ) + RE_WHITESPACE + capture( RE_NUMBER ) + RE_WHITESPACE
RE_TRANSFER_CREDIT = capture( LBL_TRANSFER_CREDIT ) + RE_WHITESPACE + capture( RE_NUMBER ) + RE_WHITESPACE
RE_LINE_ITEM = capture( RE_LABEL ) + RE_SPACES + capture( RE_NUMBER ) + RE_SPACES + 'x' + RE_SPACES + capture( RE_NUMBER ) + RE_OPTIONAL_SPACE + capture( RE_UNIT ) + RE_SPACES + capture( RE_NUMBER )


# Column name suffixes
_DLRS = '_$'
_RATE = '_rate' + _DLRS
_USED = '_used'


# Dataframe column names
ACCOUNT_NUMBER = 'account_number'
ISSUE_DATE = 'issue_date'
START_DATE = 'start_date'
END_DATE = 'end_date'
DESCRIPTION = 'description'
ADDRESS_LINE_ = 'address_line_'
CITY_STATE_ZIP = 'city_state_zip'
METER_NUMBER = 'meter_number'
TOTAL_ENERGY = 'total_energy'
KWH_USED = 'kwh' + _USED
RATE_CLASS = 'rate_class'
VOLTAGE_LEVEL = 'voltage_level'
LOADZONE = 'loadzone'
KW_USED = 'kw' + _USED
KW_KVA_USED = 'kw_kva' + _USED
CUR_CHG_NG_SERV = 'cur_chg_ng_serv_$'
CUR_CHG_OTHER_SERV = 'cur_chg_other_serv_$'
CUR_CHG_ADJUST = 'cur_chg_adjust_$'
CUR_CHG_TOTAL = 'cur_chg_total_$'
CUSTOMER_CHG = 'customer_chg_$'
LATE_PAYMENT_CHG = 'late_payment_chg_$'
SUPPLIER = 'supplier'



# Generate names of bill attributes found in an 'X' table row
def make_line_item_names( s_name ):
    return [ s_name + _DLRS, s_name + _RATE, s_name + _USED, ]

LS_BILL_ATTRS = []
LS_BILL_ATTRS.extend( \
    [
        ACCOUNT_NUMBER,
        ISSUE_DATE,
        START_DATE,
        END_DATE,
        DESCRIPTION,
        ADDRESS_LINE_ + '1',
        ADDRESS_LINE_ + '2',
        ADDRESS_LINE_ + '3',
        ADDRESS_LINE_ + '4',
        ADDRESS_LINE_ + '5',
        ADDRESS_LINE_ + '6',
        ADDRESS_LINE_ + '7',
        ADDRESS_LINE_ + '8',
        ADDRESS_LINE_ + '9',
        CITY_STATE_ZIP,
        METER_NUMBER,
        TOTAL_ENERGY,
        KWH_USED,
        RATE_CLASS,
        VOLTAGE_LEVEL,
        LOADZONE,
        KW_USED,
        KW_KVA_USED,
    ]
)
LS_BILL_ATTRS.extend( make_line_item_names( 'peak_kwh' ) )
LS_BILL_ATTRS.extend( \
    [
        CUR_CHG_NG_SERV,
        CUR_CHG_OTHER_SERV,
        CUR_CHG_ADJUST,
        CUR_CHG_TOTAL,
        CUSTOMER_CHG,
        LATE_PAYMENT_CHG,
    ]
)
LS_BILL_ATTRS.extend( make_line_item_names( 'dist_chg_kwh' ) )
LS_BILL_ATTRS.extend( make_line_item_names( 'dist_chg_off_peak_kwh' ) )
LS_BILL_ATTRS.extend( make_line_item_names( 'dist_chg_on_peak_kwh' ) )
LS_BILL_ATTRS.extend( make_line_item_names( 'dist_demand_chg_kw' ) )
LS_BILL_ATTRS.extend( make_line_item_names( 'dist_demand_chg_kw_kva' ) )
LS_BILL_ATTRS.extend( make_line_item_names( 'transition_chg_kwh' ) )
LS_BILL_ATTRS.extend( make_line_item_names( 'transmission_chg_kwh' ) )
LS_BILL_ATTRS.extend( \
    [
        SUPPLIER,
    ]
)
LS_BILL_ATTRS.extend( make_line_item_names( 'electricity_supply_kwh' ) )
LS_BILL_ATTRS.extend( make_line_item_names( 'energy_efficiency_chg_kwh' ) )
LS_BILL_ATTRS.extend( make_line_item_names( 'distributed_solar_chg_kwh' ) )
LS_BILL_ATTRS.extend( make_line_item_names( 'net_meter_recovery_chg_kwh' ) )
LS_BILL_ATTRS.extend( make_line_item_names( 'net_met_cr_kwh' ) )
LS_BILL_ATTRS.extend( make_line_item_names( 'renewable_energy_chg_kwh' ) )
LS_BILL_ATTRS.extend( make_line_item_names( 'electric_vehicle_chg_kwh' ) )
LS_BILL_ATTRS.extend( make_line_item_names( 'high_voltage_discount_kw' ) )
LS_BILL_ATTRS.extend( make_line_item_names( 'service_quality_credit_kwh' ) )
LS_BILL_ATTRS.extend( make_line_item_names( 'basic_service_fixed_kwh' ) )
LS_BILL_ATTRS.extend( make_line_item_names( 'basic_service_variable_kwh' ) )



# Split concatenated bills to individual PDF files
def split_concatenated_bills( input_dir, filename, output_dir, n_bills ):

    # Format input PDF full path
    input_pdf = os.path.join( input_dir, filename )

    # Open the original PDF, containing 1 or more bills
    original_doc = fitz.open( input_pdf )

    # Identify start page of each bill by presence of label which uniquely appears on every first page
    ls_start_pages = []
    for n_page, page in enumerate( original_doc ):
        if LBL_CUSTOMER_SERVICE in page.get_text():
            ls_start_pages.append( n_page )

    # Add one final index to artificially represent the end of the last bill
    n_start_pages = len( ls_start_pages )
    ls_start_pages.append( len( original_doc ) )

    # Split bills and and save to output directory
    for n_idx in range( n_start_pages ):

        # Initialize empty PDF document for current bill
        bill_doc = fitz.open()

        # Populate new PDF document with applicable pages
        n_start = ls_start_pages[n_idx]
        n_stop = ls_start_pages[n_idx + 1]
        for n_page in range( n_start, n_stop ):
            bill_doc.insert_pdf( original_doc, from_page=n_page, to_page=n_page )

        # Save current bill to output file
        output_filename = f'bill_{( n_idx + n_bills + 1 ):05d}.pdf'
        output_path = os.path.join( output_dir, output_filename )
        bill_doc.save( output_path )
        bill_doc.close()

    original_doc.close()

    # Report number of bills found in original PDF file
    s_quote = "'" if ' ' in filename else ''
    print( f' {n_start_pages} in {s_quote}{filename}{s_quote}' )

    # Track total number of bills
    n_bills += n_start_pages

    return n_bills


# Split concatenated bills and save individually to temp directory
def split_bills_to_temp_dir( input_dir ):

    # Create empty temp directory
    temp_dir = os.path.join( input_dir, TEMP_DIR_NAME )
    if os.path.exists( temp_dir ):
        shutil.rmtree( temp_dir )
    os.makedirs( temp_dir )

    # Track total number of bills saved in temp directory
    n_bills = 0

    print( 'Bills found:' )

    # Iterate over files in input directory
    for filename in os.listdir( input_dir ):

        if filename.lower().endswith( '.pdf' ):

            n_bills = split_concatenated_bills( input_dir, filename, temp_dir, n_bills )

            # Debug mode: quit early
            if B_DEBUG and ( n_bills > N_DEBUG ):
                break

    print( '' )
    print( f'Total bills found: {n_bills}' )
    print( '' )

    return temp_dir


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
def get_account_number( ls_lines ):

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
def get_service_address( ls_lines ):

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


# Extract date issued from National Grid bill content
def get_issue_date( ls_lines ):

    # Find first occurrence of label
    n_line = ls_lines.index( LBL_DATE_BILL_ISSUED )

    # Extract date from next line
    s_issue_date = datetime.strptime( ls_lines[n_line + 1], '%b %d, %Y' ).strftime( '%Y-%m-%d' )

    return s_issue_date


# Extract billing period from bill content
def get_billing_period( ls_lines ):

    # Find first occurrence of label
    n_line = ls_lines.index( LBL_BILLING_PERIOD )
    ls_lines = ls_lines[n_line:]

    ls_dates = ls_lines[1].split( ' to ' )
    s_start_date = datetime.strptime( ls_dates[0], '%b %d, %Y' ).strftime( '%Y-%m-%d' )
    s_end_date = datetime.strptime( ls_dates[1], '%b %d, %Y' ).strftime( '%Y-%m-%d' )

    return s_start_date, s_end_date


# Generate column name from label string
def make_column_name( s_label ):

    # Normalize label words that have inconsistent spellings
    for s_word in LABEL_WORDS.keys():
        s_label = s_label.replace( s_word, LABEL_WORDS[s_word] )

    # Change case and delimiters
    s_column_name = s_label.lower().replace( ' ', '_' )

    return s_column_name


# Save charges in dictionary
def charges_to_dc_values( matches, dc_values ):

    for m in matches:

        # First match is label; make it into a column name
        s_column_name = make_column_name( m[0] )

        # Middle matches, if present, are rate, usage, and unit
        if len( m ) >= 5:

            # Incorporate unit into column name
            s_unit = m[3].lower().replace( '/', '_' )
            s_column_name += '_' + s_unit

            # Save rate
            dc_values[s_column_name + _RATE] = float( m[1] )

            # Save usage for this line item
            f_value = float( m[2] )
            dc_values[s_column_name + _USED] = f_value

            # Save usage applicable to current unit: select maximum value among line items
            s_key = s_unit + _USED
            if s_key in dc_values:
                dc_values[s_key] = max( dc_values[s_key], f_value )
            else:
                dc_values[s_key] = f_value

        # Last match is dollar amount charged
        dc_values[s_column_name + _DLRS] = comma_float( m[-1] )

    return dc_values


# Extract values of interest from bill content
def get_bill_values( filepath ):

    # Initialize empty dictionary of bill values
    dc_values = {}

    # Open the file
    with pdfplumber.open( filepath ) as pdf:

        # Iterate over PDF pages
        for page in pdf.pages:

            text = page.extract_text()

            # Extract current charges
            matches = re.findall( RE_CURRENT_CHARGES, text )
            if matches:
                m = matches[0]
                dc_values[CUR_CHG_NG_SERV] = comma_float( m[1] )
                dc_values[CUR_CHG_OTHER_SERV] = comma_float( m[2] )
                dc_values[CUR_CHG_ADJUST] = comma_float( m[3] )
                dc_values[CUR_CHG_TOTAL] = comma_float( m[4] )

            # Extract customer charge
            matches = re.findall( RE_CUSTOMER_CHARGE, text )
            if matches:
                charges_to_dc_values( matches, dc_values )

            # Extract late charge
            matches = re.findall( RE_LATE_PAYMENT_CHARGE, text )
            if matches:
                charges_to_dc_values( matches, dc_values )

            # Extract transfer of remote net meter credit
            matches = re.findall( RE_TRANSFER_CREDIT, text )
            if matches:
                charges_to_dc_values( matches, dc_values )

            # Extract line items
            matches = re.findall( RE_LINE_ITEM, text )
            if matches:
                charges_to_dc_values( matches, dc_values )

            # Extract total energy
            matches = re.findall( RE_TOTAL_ENERGY, text )
            for m in matches:
                dc_values[TOTAL_ENERGY] = m

            # Extract meter number
            matches = re.findall( RE_METER_NUMBER, text )
            for m in matches:
                dc_values[METER_NUMBER] = m

            # Extract rate and voltage level
            matches = re.findall( RE_RATE_VOLTAGE, text )
            if matches:
                m = matches[0]
                dc_values[RATE_CLASS] = m[0]
                dc_values[VOLTAGE_LEVEL] = m[1]

            # Extract supplier
            matches = re.findall( RE_SUPPLIER, text )
            for m in matches:
                if m.startswith( 'National G' ):
                    # Kludge to remove random spaces from word 'Grid'
                    ls_parts = m.split()
                    m = ' '.join( [ls_parts[0], ''.join( ls_parts[1:] )] )
                dc_values[SUPPLIER] = m

            # Extract loadzone
            matches = re.findall( RE_LOADZONE, text )
            for m in matches:
                dc_values[LOADZONE] = m

    return dc_values


# Construct dataframe from list of bills
def make_df_bills( ls_bills, b_number_columns ):

    df = pd.DataFrame( ls_bills )

    # Sort trailing columns (if any) alphabetically
    n_leading = len( LS_BILL_ATTRS )
    ls_cols = list( df.columns )
    n_cols = len( ls_cols )
    n_start = n_leading - n_cols

    if n_start < 0:
        ls_leading = list( df.columns[:n_start] )
        ls_trailing = sorted( ls_cols[n_start:] )
    else:
        ls_leading = ls_cols
        ls_trailing = []

    ls_reordered = ls_leading + ls_trailing

    df = df.reindex( columns=ls_reordered )

    # Drop empty columns and duplicates
    df = df.dropna( axis='columns', how='all' )
    df = df.drop_duplicates()

    # Sort dataframe on account number and date
    df = df.sort_values( by=[ACCOUNT_NUMBER, ISSUE_DATE, START_DATE, END_DATE] )

    # Number columns
    if b_number_columns:
        n_cols = len( df.columns )
        n_digits = len( str( n_cols ) )
        n_col = 0
        for col in df.columns:
            n_col += 1
            df = df.rename( columns={ col: str( n_col ).zfill( n_digits ) + '-' + col } )

    return df


# Fix numeric columns in specified dataframe
def fix_numeric_columns( df ):

    for column_name in df.columns:

        if df[column_name].dtype == object:
            try:
                df[column_name] = pd.to_numeric( df[column_name], errors='ignore' )
            except:
                pass

    return df


# Map pandas datatype to SQLite datatype
def pdtype_to_sqltype( df, col_name ):

    if df is None:
        sqltype = 'TEXT'
    else:
        pdtype = str( df[col_name].dtype )

        if pdtype.startswith( 'float' ):
            sqltype = 'FLOAT'
        elif pdtype.startswith( 'int' ):
            sqltype = 'INT'
        else:
            sqltype = 'TEXT'

    return sqltype


# Save dataframe to SQLite database
def df_to_db( df, file_path, table_name ):

    # Fix numeric columns
    df = fix_numeric_columns( df )

    # Open the database
    conn = sqlite3.connect( file_path )
    cur = conn.cursor()

    # Drop table if it already exists
    cur.execute( 'DROP TABLE IF EXISTS "' + table_name + '"' )

    # Generate SQL command
    create_sql = 'CREATE TABLE "' + table_name + '" ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE'
    for col_name in df.columns:
        sqltype = pdtype_to_sqltype( df, col_name )
        create_sql += ', "{0}" {1}'.format( col_name, sqltype )
    create_sql += ' )'

    # Execute the SQL command
    print( '' )
    print( create_sql )
    cur.execute( create_sql )

    # Output dataframe to database
    df.to_sql( table_name, conn, if_exists='append', index=False )

    conn.commit()


###############################



# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Extract data from PDF-format electric bills' )
    parser.add_argument( '-i', dest='input_directory', default=os.getcwd(), help='Input directory containing electric bills' )
    parser.add_argument( '-c', dest='csv_output_filename', help='Full path to output CSV file' )
    parser.add_argument( '-x', dest='xlsx_output_filename', help='Full path to output Excel file' )
    parser.add_argument( '-s', dest='sqlite_output_filename', help='Full path to output SQLite file' )
    parser.add_argument( '-t', dest='table_name', default='ElectricBills', help='Name of SQLite table' )
    parser.add_argument( '-n', dest='number_columns', action='store_true', help='Number columns?' )
    args = parser.parse_args()

    # Process output arguments
    csv_filename = args.csv_output_filename if args.csv_output_filename is not None else ''
    xlsx_filename = args.xlsx_output_filename if args.xlsx_output_filename is not None else ''
    sqlite_filename = args.sqlite_output_filename if args.sqlite_output_filename is not None else ''

    if csv_filename + xlsx_filename + sqlite_filename == '':
        print( 'Please specify at least one of: -c, -x, -s' )
        exit()

    # Report input and outputs
    print( '' )
    print( f'Reading electric bills from: "{args.input_directory}"' )
    print( '' )
    print( f'Saving bill attributes to:' )
    if csv_filename:
        print( f' "{csv_filename}"' )
    if xlsx_filename:
        print( f' "{xlsx_filename}"' )
    if sqlite_filename:
        print( f' "{sqlite_filename}", table "{args.table_name}"' )
    print( '' )

    # Report start time
    print( 'Starting at', time.strftime( '%H:%M:%S', time.localtime( START_TIME ) ) )
    print( '' )


    # Split concatenated bills and save individual bills in temporary directory
    temp_dir = split_bills_to_temp_dir( args.input_directory )

    #
    # Extract information from individual bills
    #

    ls_bills = []

    n_file = 0

    # Iterate over individual bills in temporary directory
    for filename in os.listdir( temp_dir ):

        if filename.lower().endswith( '.pdf' ):

            n_file += 1

            # Debug mode: quit early
            if B_DEBUG and ( n_file > N_DEBUG ):
                break

            # Find the file
            filepath = os.path.join( temp_dir, filename )

            # Extract lines of text
            ls_lines = get_lines( filepath )

            # Extract National Grid account number
            s_account_number = get_account_number( ls_lines )

            # Extract bill dates
            s_issue_date = get_issue_date( ls_lines )
            s_start_date, s_end_date = get_billing_period( ls_lines )

            # Extract service address
            s_descr, ls_address_lines = get_service_address( ls_lines )

            # Report bill details
            print( f'Bill {n_file}' )
            print( f' Account: {s_account_number}' )
            print( f' Description: <{s_descr}>, Address: {ls_address_lines}' )
            print( f' Issued {s_issue_date} for period {s_start_date} to {s_end_date}' )

            # Construct dictionary from bill attributes
            dc_bill = dict.fromkeys( LS_BILL_ATTRS )
            dc_bill[ACCOUNT_NUMBER] = s_account_number
            dc_bill[START_DATE] = s_start_date
            dc_bill[END_DATE] = s_end_date
            dc_bill[ISSUE_DATE] = s_issue_date
            dc_bill[DESCRIPTION] = s_descr
            dc_bill[CITY_STATE_ZIP] = ls_address_lines[-1]

            ls_address_lines = ls_address_lines[:-1]
            n_address_line = 0
            for s_line in ls_address_lines:
                n_address_line += 1
                dc_bill[ADDRESS_LINE_ + str( n_address_line )] = s_line

            # Extract and add other bill values
            dc_values = get_bill_values( filepath )
            for s_key in dc_values.keys():
                dc_bill[s_key] = dc_values[s_key]

            # Append dictionary representing current bill to list of bills
            ls_bills.append( dc_bill )


    # Construct dataframe from list of bills
    df_bills = make_df_bills( ls_bills, args.number_columns )

    print( '' )

    # Save to CSV file
    if args.csv_output_filename is not None:
        print( f'Saving bills to "{args.csv_output_filename}"' )
        df_bills.to_csv( args.csv_output_filename, index=False )

    # Save to Excel file
    if args.xlsx_output_filename is not None:
        print( f'Saving bills to "{args.xlsx_output_filename}"' )
        df_bills.to_excel( args.xlsx_output_filename, index=False )

    # Save to SQLite database
    if args.sqlite_output_filename is not None:
        print( f'Saving bills to "{args.sqlite_output_filename}", table "{args.table_name}"' )
        df_to_db( df_bills, args.sqlite_output_filename, args.table_name )


    # Clean up
    if os.path.exists( temp_dir ):
        shutil.rmtree( temp_dir )

    # Report elapsed time
    report_elapsed_time()
