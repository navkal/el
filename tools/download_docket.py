# Copyright 2025 Energize Andover.  All rights reserved.

######################
#
# Download files from a specified docket into a collection
# of subdirectories, organized by date and filer
#
# Required parameters:
# -n <docket_number>
#
# Optional parameters:
# -d <date>
# -f <filer>
# -t <target directory> (defaults to working directory)
# -c <count_only>
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
LINKS = 'links'
LINK_COUNT = 'link_count'
DOWNLOADED = 'downloaded'
COLUMNS = [ DATE, FILER, LINKS, LINK_COUNT, DOWNLOADED ]
ROW = dict( ( el, 0 ) for el in COLUMNS )
LINK_DELIMITER = ','
SORT_1 = 'sort_1'
SORT_2 = 'sort_2'

# Database constants
TABLE_NAME = 'Filings'

# Date formats used in input, on web page, and in output directory name
DATE_FORMAT_PARSE = '%m/%d/%Y'
DATE_FORMAT_WEB = '%#m/%#d/%Y'
DATE_FORMAT_DIR = '%Y-%m-%d'

MAX_RETRY_SECONDS = 15
DB_RETRY_MIN_SEC = 10
DB_RETRY_MAX_SEC = DB_RETRY_MIN_SEC + 20

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
def make_target_directory( target_dir, docket_number, b_count_only ):
    target_dir = os.path.join( target_dir, docket_number )
    if not b_count_only:
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


# Get docket filings
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


# Get docket filings
def get_filings( db_filepath, s_date, s_filer, b_count_only ):

    print( '' )

    # If processing full docket, retrieve filings from database
    df_stored = pd.DataFrame( columns=COLUMNS )
    if not ( b_count_only or s_date or s_filer ):
        if os.path.exists( db_filepath ):
            conn, cur, engine = open_database( db_filepath )
            df_stored = pd.read_sql_table( TABLE_NAME, engine, index_col='id' )

    # Scrape filings from website
    df_scraped = pd.DataFrame( columns=COLUMNS )
    df_scraped = scrape_filings( s_date, s_filer, df_scraped )

    # Set up dataframe of all filings to be processed
    if len( df_stored ):

        # Sort the dataframes
        df_scraped = sort_filings( df_scraped )
        df_stored = sort_filings( df_stored )

        # Determine most recent stored date
        newest_stored_date = df_stored.iloc[0][SORT_1]

        # Combine scraped and stored dataframes based on recent date
        df_scraped = df_scraped[df_scraped[SORT_1] >= newest_stored_date]
        df_stored = df_stored[df_stored[SORT_1] < newest_stored_date]
        df_filings = pd.concat( [df_scraped, df_stored], ignore_index=True )
        df_filings = df_filings.drop( columns=[SORT_1, SORT_2] )

    else:
        df_filings = df_scraped

    df_filings = df_filings.reset_index( drop=True )

    return df_filings


# Sort filings based on date and filer
def sort_filings( df ):

    # Sort
    df[SORT_1] = pd.to_datetime( df[DATE] )
    df[SORT_2] = df[FILER]
    df = df.sort_values( by=[SORT_1, SORT_2], ascending=[False, True] )

    return df


# Scrape docket filings from website
def scrape_filings( s_date, s_filer, df_filings, page_number=1 ):

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
                ls_links.append( el_anchor.get_attribute( 'href' ) )

            # If the current filing contains any links, save it
            if ls_links:
                ROW[DATE] = s_date_value
                ROW[FILER] = s_filer_value
                ROW[LINKS] = LINK_DELIMITER.join( ls_links )
                ROW[LINK_COUNT] = len( ls_links )
                ROW[DOWNLOADED] = False
                df_filings = pd.concat( [df_filings, pd.DataFrame( [ROW] )], ignore_index=True )

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
            df_filings = scrape_filings( s_date, s_filer, df_filings, page_number=(page_number+1) )

    return df_filings


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


def report_counts( df_filings ):

    print( '' )
    print( 'To be processed:' )
    print( '  Filings:', len( df_filings ) )
    print( '  Documents:', df_filings[LINK_COUNT].sum() )

    # Optionally report partial progress
    df_done = df_filings[df_filings[DOWNLOADED] == True]

    if len( df_done ):
        df_to_do = df_filings[df_filings[DOWNLOADED] == False]
        print( '  Downloads done:', df_done[LINK_COUNT].sum() )
        print( '  Downloads to do:', df_to_do[LINK_COUNT].sum() )

    return


# Download files to specified target directory
def download_files( df_filings, target_dir, docket_number, db_filepath, s_date, s_filer ):

    filename = None
    count = 0
    prev_download_dir = ''

    # Extract filings to be downloaded
    df_download = df_filings[ df_filings[DOWNLOADED] == False ]

    # Iterate over filings to be downloaded
    for index, filing in df_download.iterrows():

        # Generate a directory name based on this row's date and filer and append to target directory
        dir_name = make_dir_name( filing, docket_number )
        download_dir = os.path.join( target_dir, dir_name )

        # Create the directory if it does not already exist
        if not os.path.exists( download_dir ):
            os.makedirs( download_dir )

        # Report next download directory
        if download_dir != prev_download_dir:
            print( '' )
            print( 'Downloading files to "{}"'.format( download_dir ) )
            prev_download_dir = download_dir

        # Initialize flag indicating that all files in current filing have been downloaded
        b_downloaded_all_documents_in_filing = True

        # Iterate over list of links
        for link in filing[LINKS].split( LINK_DELIMITER ):

            count += 1

            try:
                # Request the download in a retry loop
                for sec in range( 1, MAX_RETRY_SECONDS + 1 ):
                    try:
                        # Issue the request
                        response = requests.get( link, stream=True )
                        response.raise_for_status()
                    except:
                        # Request failed
                        if sec < MAX_RETRY_SECONDS:
                            # Sleep and try again
                            time.sleep( sec )
                        else:
                            # We're out of luck
                            response.raise_for_status()
                    else:
                        # Request succeeded; exit loop
                        break

                    # Wait and retry
                    time.sleep( 5 )
                    response = requests.get( link, stream=True )
                    response.raise_for_status()

                # Extract filename from content disposition header
                filename = get_filename( response.headers['content-disposition'], download_dir )

                if filename:

                    # Save the download if it doesn't already exist
                    filepath = os.path.join( download_dir, filename )
                    if os.path.exists( filepath ):
                        s_exists = ' (already exists)'
                    else:
                        s_exists = ''
                        with open( filepath, 'wb' ) as file:
                            for chunk in response.iter_content( chunk_size=8192 ):  # Adjust chunk_size as needed
                                file.write( chunk )

                    # Report success
                    print( '{: >5d}: {}{}'.format( count, filename, s_exists ) )

                else:
                    # Report error
                    print( '       Error: {}'.format( 'Filename not found in content disposition header' ) )

            except Exception as e:

                # Note that we have failed to download at least one document from current filing
                b_downloaded_all_documents_in_filing = False

                # Report error
                print( '{: >5d}: Download failed. {}'.format( count, e ) )

                # Proceed to next link?
                b_continue = \
                    ( isinstance( e, requests.exceptions.HTTPError ) and ( response.status_code == 500 ) ) # Internal Server Error, occurs when link is not valid

                if not b_continue:
                    driver.quit()
                    exit()

        # Mark download status of the current filing
        df_filings.loc[index, DOWNLOADED] = b_downloaded_all_documents_in_filing

        # Save current state in database
        if not s_date and not s_filer:

            # Invoke save operation in retry loop, to accommodate high latency storage
            for sec in range( DB_RETRY_MIN_SEC, DB_RETRY_MAX_SEC + 1 ):
                try:
                    # Issue the request
                    save_progress( db_filepath, df_filings )
                except:
                    # Request failed
                    if sec < DB_RETRY_MAX_SEC:
                        # Sleep and try again
                        time.sleep( sec )
                    else:
                        # Last chance, no exception handler
                        time.sleep( sec )
                        save_progress( db_filepath, df_filings )
                else:
                    # Request succeeded; exit loop
                    break


    if count == 0:
        print( '' )
        print( 'No files to download.' )

    return df_filings


# Open the SQLite database
def open_database( db_pathname ):

    conn = sqlite3.connect( db_pathname )
    cur = conn.cursor()

    engine = sqlalchemy.create_engine( 'sqlite:///' + db_pathname )

    return conn, cur, engine


# Save current state of full-docket download
def save_progress( db_filepath, df_filings ):

    # Open the database
    conn, cur, engine = open_database( db_filepath )

    # Drop table if it already exists
    cur.execute( 'DROP TABLE IF EXISTS ' + TABLE_NAME )

    # Generate SQL command to create table
    create_sql = 'CREATE TABLE ' + TABLE_NAME + ' ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE'
    for col_name in df_filings.columns:
        create_sql += ', "{0}"'.format( col_name )
    create_sql += ' )'

    # Create the empty table
    cur.execute( create_sql )

    # Load data into table
    df_filings.to_sql( TABLE_NAME, conn, if_exists='append', index=False )

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
    parser.add_argument( '-c', dest='count_only', action='store_true', help='Only report count' )
    args = parser.parse_args()

    # Report argument list
    print( '' )
    print( 'Arguments:',  )
    print( '  Docket Number: {}'.format( args.docket_number ) )
    print_optional_argument( 'Date', args.date )
    print_optional_argument( 'Filer', args.filer )
    print( '  Target Directory: {}'.format( args.target_directory ) )
    print( '  Count Only: {}'.format( args.count_only ) )

    # Report start time
    print( '' )
    print( 'Starting at', time.strftime( '%H:%M:%S', time.localtime( START_TIME ) ) )

    # Ensure that date is in format used by docket website
    s_date = format_date( args.date ) if args.date else ''

    # Create target directory for downloads, named for the docket number
    target_dir = make_target_directory( args.target_directory, args.docket_number, args.count_only )

    # Get the Chrome driver
    driver = get_driver( target_dir )

    # Get the web page for the specified docket
    get_docket( args.docket_number, target_dir )

    # Save docket description
    if not args.count_only:
        save_docket_description( target_dir )

    # Get docket filings
    db_filepath = os.path.join( target_dir, args.docket_number + '.sqlite' )
    df_filings = get_filings( db_filepath, s_date, args.filer, args.count_only )

    # Report counts of filings and files
    report_counts( df_filings )

    # Download files listed in identified rows
    if not args.count_only:
        df_filings = download_files( df_filings, target_dir, args.docket_number, db_filepath, s_date, args.filer )

    # Close the browser
    driver.quit()

    # Report elapsed time
    report_elapsed_time()
