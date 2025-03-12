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


# Date formats used on web page and output directory
DATE_FORMAT_WEB = '%m/%d/%Y'
DATE_FORMAT_DIR = '%Y-%m-%d'

MAX_RETRY_SECONDS = 10


FILENAME_LABEL = 'filename='
UTF_LABEL = '=?utf-8?B?'
MAX_PATH_LEN = 220
REPLACEMENT_CHAR = '\uFFFD'     # Occurs in utf-8 encoded filenames


# Report value of optional argument
def print_optional_argument( s_label, s_value ):
    print( '  {}: {}'.format( s_label, s_value if s_value else '<any>' ) )


# Ensure date format mm/dd/yyyy
def format_date( s_date ):

    try:
        # Parse the date
        d = datetime.strptime( s_date, DATE_FORMAT_WEB )

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
def get_docket( driver, s_docket_number ):

    # Open the web page
    docket_url = 'https://eeaonline.eea.state.ma.us/DPU/Fileroom/dockets/bynumber/' + s_docket_number
    driver.get( docket_url )

    # Initialize xpath that would navigate to an error message, should one appear
    xpath_parts = \
    [
        # Select the div in which all results are displayed
        '//div[@class="resultsList"]',
        # Select the div containing a centered message that reports errors
        '/div[@class="resultsCenter"]',
    ]
    xpath = ''.join( xpath_parts )

    print( '' )
    print( 'Waiting for docket' )
    print( '  Docket Number: {}'.format( s_docket_number ) )

    try:
        # Wait briefly for error message to load
        element = WebDriverWait( driver, 10 ).until( EC.presence_of_element_located( ( By.XPATH, xpath ) ) )

        # Extract error message
        s_error = element.get_attribute( 'innerHTML' )

        # Report error message
        print( '' )
        print( 'Error loading docket' )
        print( '  URL: {}'.format( docket_url ) )
        print( '  Reason: {}'.format( s_error ) )
        print( '' )
        print( '--> Please check requested docket number' )

        exit()

    except TimeoutException:
        # No error message; do nothing
        pass

    return


# Get IDs of rows with specified date and filer
def get_row_ids( driver, s_date, s_filer ):

    # Initialize xpath to navigate to the docket row representing the requested date and filer
    xpath_parts = \
    [
        # Select all the docket rows
        '//div[@class="divGridRow"]',
        # Select the left column of each row
        '/div[@class="left"]',
        # Select the left-column cells that have a matching date
        '[.//text()[contains( ., "' + s_date + '" )]]',
        # Navigate back to the parent rows, now filtered by date
        '/parent::div',
        # Select the right column of those rows
        '/div[@class="right"]',
        # Select the element containing the filer
        '/span[@class="filer"]',
        # Select the right-column cells that have a matching filer
        '[.//text()[contains( ., "' + s_filer + '" )]]',
        # Navigate back to the parent row, now filtered by date and filer
        '/parent::div',
        '/parent::div',
    ]
    xpath = ''.join( xpath_parts )

    print( '' )
    print( 'Waiting for filings' )
    print_optional_argument( 'Date', s_date )
    print_optional_argument( 'Filer', s_filer )

    # Wait for the row with requested date and filer to load
    try:
        ls_elements = WebDriverWait( driver, 90 ).until( EC.presence_of_all_elements_located( ( By.XPATH, xpath ) ) )
        ls_row_ids = []
        for element in ls_elements:
            ls_row_ids.append( element.get_attribute( 'id' ) )

    except TimeoutException:
        print( '' )
        print( 'Request timed out.' )
        print( '' )
        if s_date or s_filer:
            print( '--> Please check argument values' )
            print_optional_argument( 'Date', s_date )
            print_optional_argument( 'Filer', s_filer )
        exit()

    # Reverse the order, so in case of duplication, newer files overwrite older files
    return reversed( ls_row_ids )


# Download files to specified target directory
def download_files( driver, ls_row_ids, target_dir ):

    filename = None

    count = 0

    prev_download_dir = ''

    for row_id in ls_row_ids:

        # Generate a directory name based on this row's date and filer and append to target directory
        dir_name = make_dir_name( row_id )
        download_dir = os.path.join( target_dir, dir_name )

        # Create the directory if it does not already exist
        if not os.path.exists( download_dir ):
            os.makedirs( download_dir )

        # Report next download directory
        if download_dir != prev_download_dir:
            print( '' )
            print( 'Downloading files to "{}":'.format( download_dir ) )
            prev_download_dir = download_dir

        # Initialize xpath that would navigate to list of download links
        xpath_parts = \
        [
            # Select the row
            '//div[@id="files_' + row_id + '"]',
            # Select the links in the row
            '/a',
        ]
        xpath = ''.join( xpath_parts )

        # Extract the links
        ls_links = driver.find_elements( By.XPATH, xpath )

        # Iterate over list of links
        for link in ls_links:

            count += 1

            try:
                # Request the download in a retry loop
                for sec in range( 1, MAX_RETRY_SECONDS + 1 ):
                    try:
                        # Issue the request
                        response = requests.get( link.get_attribute( 'href' ), stream=True )
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
                    response = requests.get( link.get_attribute( 'href' ), stream=True )
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
                exit()

    if count == 0:
        print( '  No files found' )


# Generate a directory name from date and filer of identified row
def make_dir_name( row_id ):

    # Find the date of the current row
    xpath_parts = \
    [
        # Select the row
        '//div[@id="' + row_id + '"]',
        # Select the left column of the row
        '/div[@class="left"]',
        # Select the element containing the date
        '/span[@class="created"]',
        # Select the element containing the date text
        '/strong',
    ]
    xpath = ''.join( xpath_parts )
    el = driver.find_element( By.XPATH, xpath )
    s_date = el.get_attribute( 'innerHTML' )

    # Find the filer of the current row
    xpath_parts = \
    [
        # Select the row
        '//div[@id="' + row_id + '"]',
        # Select the right column of the row
        '/div[@class="right"]',
        # Select the element containing the filer
        '/span[@class="filer"]',
    ]
    xpath = ''.join( xpath_parts )
    el = driver.find_element( By.XPATH, xpath )
    s_filer = el.get_attribute( 'innerHTML' )

    # Reformat date for directory name
    d = datetime.strptime( s_date, DATE_FORMAT_WEB )
    s_date = d.strftime( DATE_FORMAT_DIR )

    # Format valid directory name from combined date and filer
    dir_name = sanitize_filename( s_date + ' ' + s_filer )
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
    get_docket( driver, args.docket_number )

    # Get IDs of rows specified by date and filer
    ls_row_ids = get_row_ids( driver, s_date, args.filer )

    # Download files listed in identified rows
    download_files( driver, ls_row_ids, args.target_directory )

    # Close the browser
    driver.quit()

    # Report elapsed time
    report_elapsed_time()
