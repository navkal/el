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
from datetime import datetime

import os
import requests
import base64
from pathvalidate import sanitize_filename

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
XPATH_FILER = '//input[contains(@class, "mat-input-element") and (@type="text") and (@aria-label="Filer")]'
XPATH_COMMON_ANCESTER = '../../../../../../..'
XPATH_ANCHOR = './/a'

DATE = 'date'
FILER = 'filer'
LINKS = 'links'

# Date formats used in input, on web page, and in output directory name
DATE_FORMAT_PARSE = '%m/%d/%Y'
DATE_FORMAT_WEB = '%#m/%#d/%Y'
DATE_FORMAT_DIR = '%Y-%m-%d'

MAX_RETRY_SECONDS = 10

FILENAME_LABEL = 'filename='
UTF_LABEL = '=?utf-8?B?'
MAX_PATH_LEN = 220
REPLACEMENT_CHAR = '\uFFFD'     # Occurs in utf-8 encoded filenames


# Report value of optional argument
def print_optional_argument( s_label, s_value ):
    print( '  {}: {}'.format( s_label, s_value if s_value else '<any>' ) )


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
def get_docket( s_docket_number ):

    # Open the dashboard
    driver.get( DASHBOARD_URL )

    print( '' )
    print( 'Waiting for dashboard' )

    try:
        # Wait for dashboard to load
        WebDriverWait( driver, 5 ).until( EC.presence_of_element_located( ( By.XPATH, XPATH_DASHBOARD ) ) )
    except:
        # Report error and abort
        print( '' )
        print( 'Error loading dashboard' )
        print( '  URL: {}'.format( DASHBOARD_URL ) )
        print( '' )
        driver.quit()
        exit()

    # Set docket number.  This triggers the website to load the docket.
    input_element = driver.find_element( By.ID, 'mat-input-1' )
    input_element.send_keys( s_docket_number )

    return


# Get docket filings
def get_filings( s_date, s_filer, ls_filings, page_number=1 ):

    print( f'Waiting for filings, page {page_number}' )

    # Wait for filer elements to load
    try:
        ls_filers = WebDriverWait( driver, 10 ).until( EC.presence_of_all_elements_located( ( By.XPATH, XPATH_FILER ) ) )
    except:
        # Report error and abort
        print( '' )
        print( 'Error loading filings' )
        print( '-- Check your arguments --' )
        print( '  Docket Number: {}'.format( args.docket_number ) )
        print_optional_argument( 'Date', s_date )
        print_optional_argument( 'Filer', s_filer )
        print( '' )
        driver.quit()
        exit()

    # Iterate over list of filers
    for el_filer in ls_filers:

        # Navigate to common ancestor
        el_ancestor = el_filer.find_element( By.XPATH, XPATH_COMMON_ANCESTER )

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

            # Save the current filing
            ls_filings.append(
                {
                    DATE: s_date_value,
                    FILER: s_filer_value,
                    LINKS: ls_links,
                }
            )


    # Find the Next Page button
    next_button = driver.find_element( By.CSS_SELECTOR, 'button.mat-paginator-navigation-next' )

    # Scroll the button into view
    driver.execute_script( 'arguments[0].scrollIntoView(true);', next_button)

    # If the button is enabled, load and process the next page
    if next_button.is_enabled():
        next_button.click()
        ls_filings = get_filings( s_date, s_filer, ls_filings, page_number=(page_number+1) )

    return ls_filings


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


# Download files to specified target directory
def download_files( ls_filings, target_dir ):

    filename = None

    count = 0

    prev_download_dir = ''

    for filing in reversed( ls_filings ):

        # Generate a directory name based on this row's date and filer and append to target directory
        dir_name = make_dir_name( filing )
        download_dir = os.path.join( target_dir, dir_name )

        # Create the directory if it does not already exist
        if not os.path.exists( download_dir ):
            os.makedirs( download_dir )

        # Report next download directory
        if download_dir != prev_download_dir:
            print( '' )
            print( 'Downloading files to "{}":'.format( download_dir ) )
            prev_download_dir = download_dir

        # Iterate over list of links
        for link in filing[LINKS]:

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
                    # Save the download
                    with open( os.path.join( download_dir, filename ), 'wb' ) as file:
                        for chunk in response.iter_content( chunk_size=8192 ):  # Adjust chunk_size as needed
                            file.write( chunk )

                    # Report success
                    print( '{: >3d}: {}'.format( count, filename ) )

                else:
                    # Report error
                    print( '  Error: {}'.format( 'Filename not found in content disposition header' ) )

            except Exception as e:
                # Report error and abort
                print( '{: >3d}: {}'.format( count, filename ) )
                print( '  Download failed: {}'.format( e ) )
                driver.quit()
                exit()

    if count == 0:
        print( '  No files found' )


# Generate a directory name from date and filer of identified filing
def make_dir_name( filing ):

    # Reformat date for directory name
    d = datetime.strptime( filing[DATE], DATE_FORMAT_PARSE )
    s_date = d.strftime( DATE_FORMAT_DIR )

    # Format valid directory name from combined date and filer
    dir_name = sanitize_filename( s_date + ' ' + filing[FILER] )
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
    parser.add_argument( '-t', dest='target_directory', default=os.getcwd(), help='Target directory where downloads will be organized into subdirectories' )
    args = parser.parse_args()

    # Report argument list
    print( '' )
    print( __file__, 'running with the following arguments',  )
    print( '  Docket Number: {}'.format( args.docket_number ) )
    print_optional_argument( 'Date', args.date )
    print_optional_argument( 'Filer', args.filer )
    print( '  Target Directory: {}'.format( args.target_directory ) )

    # Report start time
    print( '' )
    print( 'Starting at', time.strftime( '%H:%M:%S', time.localtime( START_TIME ) ) )

    # Ensure that date is in format used by docket website
    s_date = format_date( args.date ) if args.date else ''

    # Get the Chrome driver
    driver = get_driver( args.target_directory )

    # Get the web page for the specified docket
    get_docket( args.docket_number )

    # Get docket filings
    ls_filings = []
    ls_filings = get_filings( s_date, args.filer, ls_filings )

    # Download files listed in identified rows
    download_files( ls_filings, args.target_directory )

    # Close the browser
    driver.quit()

    # Report elapsed time
    report_elapsed_time()

