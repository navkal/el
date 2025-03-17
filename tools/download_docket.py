# Copyright 2025 Energize Andover.  All rights reserved.

######################
#
# Download files from a specified docket into a collection
# of subdirectories, organized by date and filer.
#
# When operating on a full docket (not filtered by date or filer),
# maintain a list of filed documents in a sqlite database.
#
# Required parameters:
# -n <docket_number>
#
# Optional parameters:
# -d <date>
# -f <filer>
# -t <target directory> (defaults to working directory)
# -b <brief>
#
# Sample parameter sequences:
#
#  Download files posted on specified date by specified filer
#   -n 24-141 -d 10/31/2024 -f "Eversource Energy" -t ./out
#
#  Download all files posted on specified date
#   -n 24-141 -d 01/06/2025 -t ./out
#
#  Download all files posted by specified filer
#   -n 24-141 -f "DOER" -t ./out
#
#  Download an entire docket
#   -n 24-141 -t ./out
#
######################

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

from datetime import datetime

import os
import requests
import base64
from pathvalidate import sanitize_filename

import sqlite3
import sqlalchemy

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


# --> Reporting of elapsed time -->
import time
START_TIME = time.time()
def report_elapsed_time( prefix='\n', start_time=START_TIME ):
    elapsed_time = round( ( time.time() - start_time ) * 1000 ) / 1000
    minutes, seconds = divmod( elapsed_time, 60 )
    ms = round( ( elapsed_time - int( elapsed_time ) ) * 1000 )
    print( prefix + 'Elapsed time: {:02d}:{:02d}.{:d}'.format( int( minutes ), int( seconds ), ms ) )
# <-- Reporting of elapsed time <--


DASHBOARD_URL = 'https://eeaonline.eea.state.ma.us/dpu/fileroom/#/dashboard'

XPATH_DASHBOARD = '//input[@id="mat-input-1"]'
XPATH_DESCRIPTION = '//div[contains(@class, "text") and (@aria-label="Docket description") and (text() != "")]'
XPATH_FILER = '//input[contains(@class, "mat-input-element") and (@type="text") and (@aria-label="Filer")]'
XPATH_COMMON_ANCESTOR = '../../../../../../..'
XPATH_ANCHOR = './/a'

# Dataframe structure
DATE = 'date'
FILER = 'filer'
LABEL = 'label'
LINK = 'link'
FILENAME = 'filename'
COLUMNS = [ DATE, FILER, LABEL, LINK, FILENAME ]
ROW = dict( ( el, 0 ) for el in COLUMNS )
SORTABLE_DATE = 'sortable_date'

# Database constants
TABLE_NAME = 'Documents'

# Date formats used in input, on web page, and in output directory name
DATE_FORMAT_PARSE = '%m/%d/%Y'
DATE_FORMAT_WEB = '%#m/%#d/%Y'
DATE_FORMAT_DIR = '%Y-%m-%d'

DOC_TIMEOUT_SEC_MIN = 1
DOC_TIMEOUT_SEC_MAX = DOC_TIMEOUT_SEC_MIN + 10
DB_RETRY_SEC_MIN = 10
DB_RETRY_SEC_MAX = DB_RETRY_SEC_MIN + 10

FILENAME_LABEL = 'filename='
UTF_LABEL = '=?utf-8?B?'
MAX_PATH_LEN = 220
REPLACEMENT_CHAR = '\uFFFD'     # Occurs in utf-8 encoded filenames


# Report value of optional argument
def print_optional_argument( s_label, s_value ):
    print( '  {}: {}'.format( s_label, s_value if s_value else '<any>' ) )
    return


# Ensure date format [m]m/[d]d/yyyy
def format_date( s_date ):

    try:
        # Parse the date
        d = datetime.strptime( s_date, DATE_FORMAT_PARSE )

        # Reformat the date as mm/dd/yyyy
        s_date = d.strftime( DATE_FORMAT_WEB )

    except ValueError:
        # Could not parse date
        print( '' )
        print( 'Error in date format: {}'.format( s_date ) )
        exit()

    return s_date


# Create a target directory for downloads, based on supplied target directory and docket number
def make_target_directory( target_dir, docket_number ):
    target_dir = os.path.join( target_dir, docket_number )
    os.makedirs( target_dir, exist_ok=True )
    return target_dir


# Get the Chrome driver
def get_driver( target_dir ):

    # Set up Selenium WebDriver
    options = webdriver.ChromeOptions()
    options.add_experimental_option(
        'prefs',
        {
            'download.default_directory': target_dir,
            'download.prompt_for_download': False,
            "download.directory_upgrade": True,
            'safebrowsing.enabled': True
        }
    )
    options.add_argument( '--headless' )

    # Initialize the driver
    try:
        driver = webdriver.Chrome( service=webdriver.ChromeService( ChromeDriverManager().install() ), options=options )
    except:
        # Retry using syntax from a different Selenium version
        driver = webdriver.Chrome( ChromeDriverManager().install(), options=options )

    return driver


# Get the web page for the specified docket
def get_docket( s_docket_number, download_dir ):

    # Open the dashboard
    driver.get( DASHBOARD_URL )

    print( '' )
    print( 'Waiting for dashboard' )

    try:
        # Wait for dashboard to load
        WebDriverWait( driver, 10 ).until( EC.presence_of_element_located( ( By.XPATH, XPATH_DASHBOARD ) ) )

    except Exception as e:
        print( '' )
        print( 'Error loading dashboard.' )
        if isinstance( e, TimeoutException ):
            print( 'Request timed out.' )
        print( '' )
        print( '  URL: {}'.format( DASHBOARD_URL ) )
        print( '' )
        driver.quit()
        exit()

    # Set docket number.  This triggers the website to load the docket.
    el_input = driver.find_element( By.ID, 'mat-input-1' )
    el_input.send_keys( s_docket_number )

    return


# Save docket description in a text file
def save_docket_description( download_dir ):

    print( '' )
    print( 'Waiting for docket description' )

    try:
        # Wait for docket description to load
        WebDriverWait( driver, 30 ).until( EC.presence_of_element_located( ( By.XPATH, XPATH_DESCRIPTION ) ) )
        s_descr = driver.find_element( By.XPATH, XPATH_DESCRIPTION ).text

    except:
        msg = 'Docket description not found.'
        print( msg )
        s_descr = f'<{msg}>'

    # Save docket description in file
    filename = args.docket_number + ' docket_description.txt'
    filepath = os.path.join( download_dir, filename )
    print( '  Saving to', filepath )

    with open( filepath, 'w' ) as file:
        file.write( s_descr )

    return


# Get list of filed documents
def get_filed_documents( db_filepath, s_date, s_filer ):

    print( '' )

    # If processing full docket, retrieve list of filed documents from database
    df_stored = pd.DataFrame( columns=COLUMNS )
    if not s_date and not s_filer:
        if os.path.exists( db_filepath ):
            conn, cur, engine = open_database( db_filepath )
            df_stored = pd.read_sql_table( TABLE_NAME, engine, index_col='id' )

    # Scrape list of filed documents from website
    df_scraped = pd.DataFrame( columns=COLUMNS )
    df_scraped = scrape_filings( s_date, s_filer, df_scraped )

    # Set up dataframe of all filed documents to be processed
    if len( df_stored ):

        # Combine scraped and stored dataframes
        df_docs = pd.concat( [df_scraped, df_stored], ignore_index=True )
        df_docs = df_docs.sort_values( by=[FILENAME] )
        df_docs = df_docs.drop_duplicates( subset=[DATE, FILER, LINK], keep='last' )

    else:
        df_docs = df_scraped

    # Clean up
    df_docs[SORTABLE_DATE] = pd.to_datetime( df_docs[DATE] )
    df_docs = df_docs.sort_values( by=[SORTABLE_DATE, FILER, LABEL, LINK], ascending=[False, True, True, True] )
    df_docs = df_docs.drop( columns=[SORTABLE_DATE] )
    df_docs = df_docs.reset_index( drop=True )

    return df_docs


# Scrape list of filed documents from website
def scrape_filings( s_date, s_filer, df_docs, page_number=1 ):

    print( f'Waiting for filings, page {page_number}' )

    # Wait for filer elements to load
    try:
        ls_filers = WebDriverWait( driver, 30 ).until( EC.presence_of_all_elements_located( ( By.XPATH, XPATH_FILER ) ) )

    except Exception as e:
        print( '' )
        print( 'Error loading filings.' )
        if isinstance( e, TimeoutException ):
            print( 'Request timed out.' )
        print( '' )
        print( '  Docket Number: {}'.format( args.docket_number ) )
        print_optional_argument( 'Date', s_date )
        print_optional_argument( 'Filer', s_filer )
        print( '' )
        driver.quit()
        exit()

    # Iterate over list of filers
    for el_filer in ls_filers:

        # Navigate to common ancestor
        el_ancestor = el_filer.find_element( By.XPATH, XPATH_COMMON_ANCESTOR )

        # Find date element that corresponds with current filer element
        el_date = el_ancestor.find_element( By.CLASS_NAME, 'mat-datepicker-input' )

        # Extract text values
        s_date_value = el_date.get_attribute( 'value' )
        s_filer_value = el_filer.get_attribute( 'value' )

        if user_requested_this_filing( s_date, s_date_value, s_filer, s_filer_value ):

            # Find anchors corresponding with current filer element
            ls_anchors = el_ancestor.find_element( By.CLASS_NAME, 'mat-chip-list-wrapper' ).find_elements( By.XPATH, XPATH_ANCHOR )

            # Extract the download links
            ls_links = []
            for el_anchor in ls_anchors:
                dc_link = { LABEL: el_anchor.text, LINK: el_anchor.get_attribute( 'href' ) }
                ls_links.append( dc_link )

            # Save all links found in the current filing
            for dc_link in ls_links:
                ROW[DATE] = s_date_value
                ROW[FILER] = s_filer_value
                ROW[LABEL] = dc_link[LABEL]
                ROW[LINK] = dc_link[LINK]
                ROW[FILENAME] = ''
                df_docs = pd.concat( [df_docs, pd.DataFrame( [ROW] )], ignore_index=True )

    # Find the Next Page button
    ls_next_buttons = driver.find_elements( By.CSS_SELECTOR, 'button.mat-paginator-navigation-next' )

    # If Next Page button exists, process it
    if ls_next_buttons:
        next_button = ls_next_buttons[0]

        # Scroll the button into view
        driver.execute_script( 'arguments[0].scrollIntoView(true);', next_button)

        # If the button is enabled, load and process the next page
        if next_button.is_enabled():
            next_button.click()
            df_docs = scrape_filings( s_date, s_filer, df_docs, page_number=(page_number+1) )

    return df_docs


# Determine whether the current filing was requested via date and filer arguments
def user_requested_this_filing( s_date, s_date_value, s_filer, s_filer_value ):

    # Convert strings to lowercase before comparing
    s_date = s_date.lower()
    s_date_value = s_date_value.lower()
    s_filer = s_filer.lower()
    s_filer_value = s_filer_value.lower()

    if s_date and s_filer:
        # Both requested, both must match
        b_rq = ( s_date == s_date_value ) and ( s_filer == s_filer_value )

    elif s_date:
        # Only date requested, only date must match
        b_rq = ( s_date == s_date_value )

    elif s_filer:
        # Only filer requested, only filer must match
        b_rq = ( s_filer == s_filer_value )

    else:
        # Neither date nor filer was requested, neither must match
        b_rq = True

    return b_rq


def report_counts( df_docs ):

    print( '' )
    print( 'To be processed:' )
    print( '  Documents:', len( df_docs ) )

    # Optionally report partial progress
    n_done = len( df_docs[df_docs[FILENAME] != ''] )

    if n_done:
        n_to_do = len( df_docs[df_docs[FILENAME] == ''] )
        print( '  Downloads done:', n_done )
        print( '  Downloads to do:', n_to_do )

    return


# Download files to specified target directory
def download_files( df_docs, target_dir, docket_number, db_filepath, s_date, s_filer ):

    filename = None
    count = 0
    prev_download_dir = ''

    # Extract list of documents to be downloaded
    df_download = df_docs[ df_docs[FILENAME] == '' ]

    # Iterate over documents to be downloaded
    for index, doc in df_download.iterrows():

        count += 1

        # Generate a directory name based on this row's date and filer and append to target directory
        dir_name = make_dir_name( doc, docket_number )
        download_dir = os.path.join( target_dir, dir_name )

        # Create the directory if it does not already exist
        if not os.path.exists( download_dir ):
            os.makedirs( download_dir )

        # Report next download directory
        if download_dir != prev_download_dir:
            print( '' )
            print( 'Downloading files to "{}"'.format( download_dir ) )
            prev_download_dir = download_dir

        try:
            # Try to download current document
            response = try_to_download_document( doc[LINK] )

            # Extract filename from content disposition header
            filename = get_filename( response.headers['content-disposition'], download_dir )

            if filename:

                # Save the download if it doesn't already exist
                s_exists = save_download( download_dir, filename, response )

                # Update current document in dataframe
                df_docs.loc[index, FILENAME] = filename

                # Report success
                print( '{: >5d}: {}{}'.format( count, filename, s_exists ) )

            else:
                # Report error
                print( '       Error: {}'.format( 'Filename not found in content disposition header' ) )

        except Exception as e:

            # Report error
            print( '{: >5d}: Download failed. {}'.format( count, e ) )

            # Proceed to next link?
            b_continue = \
                ( isinstance( e, requests.exceptions.HTTPError ) ) # Internal Server Error, occurs when link is not valid

            if not b_continue:
                driver.quit()
                exit()

        # Save current state in database
        if not s_date and not s_filer:
            try_to_save_progress( db_filepath, df_docs )

    if count == 0:
        print( '' )
        print( 'No files to download.' )

    return df_docs


# Save download to disk
def save_download( download_dir, filename, response ):

    filepath = os.path.join( download_dir, filename )

    if os.path.exists( filepath ):
        s_exists = ' (already exists)'

    else:
        s_exists = ''

        with open( filepath, 'wb' ) as file:
            for chunk in response.iter_content( chunk_size=8192 ):  # Adjust chunk_size as needed
                file.write( chunk )

    return


# Try to download document from server
def try_to_download_document( link ):

    # Request the document in a retry loop
    for sec in range( DOC_TIMEOUT_SEC_MIN, DOC_TIMEOUT_SEC_MAX + 1 ):

        try:
            # Issue the request
            response = requests.get( link, stream=True, timeout=sec )
            response.raise_for_status()

        except:
            # Request failed
            if sec == DOC_TIMEOUT_SEC_MAX:
                # We're out of luck
                response.raise_for_status()

    return response


# Save current state of full-docket download
def try_to_save_progress( db_filepath, df_docs ):

    # Invoke save operation in retry loop, to accommodate high latency storage
    for sec in range( DB_RETRY_SEC_MIN, DB_RETRY_SEC_MAX + 1 ):
        try:
            # Issue the request
            save_progress( db_filepath, df_docs )
        except:
            # Request failed
            if sec < DB_RETRY_SEC_MAX:
                # Sleep and try again
                time.sleep( sec )
            else:
                # Last chance; no more retries
                time.sleep( sec )
                try:
                    save_progress( db_filepath, df_docs )
                except Exception as e:
                    print( '' )
                    print( f'Error writing to database: {e}' )
                    driver.quit()
                    exit()
        else:
            # Request succeeded; exit loop
            break

    return


# Open the SQLite database
def open_database( db_pathname ):

    conn = sqlite3.connect( db_pathname )
    cur = conn.cursor()

    engine = sqlalchemy.create_engine( 'sqlite:///' + db_pathname )

    return conn, cur, engine


# Save current state of full-docket download
def save_progress( db_filepath, df_docs ):

    # Open the database
    conn, cur, engine = open_database( db_filepath )

    # Drop table if it already exists
    cur.execute( 'DROP TABLE IF EXISTS ' + TABLE_NAME )

    # Generate SQL command to create table
    create_sql = 'CREATE TABLE ' + TABLE_NAME + ' ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE'
    for col_name in df_docs.columns:
        create_sql += ', "{0}"'.format( col_name )
    create_sql += ' )'

    # Create the empty table
    cur.execute( create_sql )

    # Load data into table
    df_docs.to_sql( TABLE_NAME, conn, if_exists='append', index=False )

    # Commit changes
    conn.commit()
    return


# Generate a directory name from date and filer of identified filing
def make_dir_name( filing, docket_number ):

    # Reformat date for directory name
    d = datetime.strptime( filing[DATE], DATE_FORMAT_PARSE )
    s_date = d.strftime( DATE_FORMAT_DIR )

    # Format valid directory name from combined date and filer
    dir_name = sanitize_filename( docket_number + ' ' + s_date + ' ' + filing[FILER] )
    return dir_name


# Extract filename from content disposition header
def get_filename( content_disposition, download_dir ):

    filename = None

    if content_disposition:

        # Split disposition into parts
        parts = content_disposition.split(";")

        # File filename part
        for part in parts:

            part = part.strip()
            if part.startswith( FILENAME_LABEL ):

                # Extract the filename
                filename = part[len( FILENAME_LABEL ):].strip( '"' )

                # Optionally decode the filename
                if filename.startswith( UTF_LABEL ):
                    filename = filename.split( '?' )[3]
                    filename = base64.b64decode( filename )
                    filename = filename.decode( 'utf-8' )

                break

    # Sanitize
    filename = sanitize_filename( filename )

    # Remove unwelcome characters
    filename = ''.join( filename.split( ',' ) )
    filename = ''.join( filename.split( REPLACEMENT_CHAR ) )

    # Truncate
    split_on_dot = filename.split( '.' )
    name_part = '.'.join( split_on_dot[:-1] )
    ext_part = split_on_dot[-1]
    len_available = MAX_PATH_LEN - len( download_dir )
    name_part = name_part[:len_available]
    filename = '.'.join( [ name_part, ext_part ] )

    return filename


######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Download files from a specified docket into a collection of subdirectories, organized by date and filer' )
    parser.add_argument( '-n', dest='docket_number',  help='Docket number', required=True )
    parser.add_argument( '-d', dest='date',  help='Date of filing', default='' )
    parser.add_argument( '-f', dest='filer',  help='Filer', default='' )
    parser.add_argument( '-t', dest='target_directory', default=os.getcwd(), help='Target directory' )
    parser.add_argument( '-b', dest='brief', action='store_true', help='Brief mode; stop before downloading any files' )
    args = parser.parse_args()

    # Report argument list
    print( '' )
    print( 'Arguments:',  )
    print( '  Docket Number: {}'.format( args.docket_number ) )
    print_optional_argument( 'Date', args.date )
    print_optional_argument( 'Filer', args.filer )
    print( '  Target Directory: {}'.format( args.target_directory ) )
    print( '  Brief: {}'.format( args.brief ) )

    # Report start time
    print( '' )
    print( 'Starting at', time.strftime( '%H:%M:%S', time.localtime( START_TIME ) ) )

    # Ensure that date is in format used by docket website
    s_date = format_date( args.date ) if args.date else ''

    # Create target directory for downloads, named for the docket number
    target_dir = make_target_directory( args.target_directory, args.docket_number )

    # Get the Chrome driver
    driver = get_driver( target_dir )

    # Get the web page for the specified docket
    get_docket( args.docket_number, target_dir )

    # Save docket description
    save_docket_description( target_dir )

    # Get list of filed documents
    db_filepath = os.path.join( target_dir, args.docket_number + '.sqlite' )
    df_docs = get_filed_documents( db_filepath, s_date, args.filer )

    # Report counts of documents
    report_counts( df_docs )

    # Decide what to do with list of filed documents
    if args.brief:
        if not os.path.exists( db_filepath ) and not s_date and not args.filer:
            # Create new database from scraped list of documents
            try_to_save_progress( db_filepath, df_docs )
    else:
        # Download listed documents
        df_docs = download_files( df_docs, target_dir, args.docket_number, db_filepath, s_date, args.filer )

    # Close the browser
    driver.quit()

    # Report elapsed time
    report_elapsed_time()
