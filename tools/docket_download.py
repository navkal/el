# Copyright 2025 Energize Andover.  All rights reserved.

######################
#
# Required parameters:
# -n <docket_number>
# -d <date>
# -f <filer>
#
# Optional parameters:
# -t <target directory> (defaults to working directory)
#
# Sample parameter sequence:
# -n 24-141 -d 10/31/2024 -f "Eversource Energy" -t ./out
#
######################


import argparse

import os
import requests

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
        '[.//text()[contains(., "' + s_date + '" )]]',
        # Navigate back to the parent rows, now filtered by date
        '/parent::div',
        # Select the right column of those rows
        '/div[@class="right"]',
        # Select the right-column cells that have a matching filer
        '[.//text()[contains(., "' + s_filer + '" )]]',
        # Navigate back to the parent row, now filtered by date and filer
        '/parent::div',
    ]
    xpath = ''.join( xpath_parts )

    print( '' )
    print( 'Waiting for filings' )
    print( '  Date: {}'.format( s_date ) )
    print( '  Filer: {}'.format( s_filer ) )

    # Wait for the row with requested date and filer to load
    try:
        ls_elements = WebDriverWait( driver, 90 ).until( EC.presence_of_all_elements_located( ( By.XPATH, xpath ) ) )
        ls_row_ids = []
        for element in ls_elements:
            ls_row_ids.append( element.get_attribute( 'id' ) )

    except TimeoutException:
        print( '' )
        print( 'Error loading requested filings' )
        print( '' )
        print( '--> Please check requested date and filer' )
        exit()

    return ls_row_ids


# Download files to specified target directory
def download_files( driver, ls_row_ids, target_dir ):

    print( '' )
    print( 'Downloading files to {}:'.format( target_dir ) )
    count = 0

    for row_id in ls_row_ids:

        # Initialize xpath that would navigate to list of download links
        xpath_parts = \
        [
            '//div[@id="files_' + row_id + '"]',
            '/a',
        ]
        xpath = ''.join( xpath_parts )

        # Extract the links
        ls_links = driver.find_elements( By.XPATH, xpath )

        # Iterate over list of links
        for link in ls_links:

            count += 1

            try:
                # Get the download
                response = requests.get( link.get_attribute( 'href' ), stream=True )

                # Raise HTTPError for bad response (4xx or 5xx)
                response.raise_for_status()

                # Extract filename from content disposition header
                filename = get_filename( response.headers['content-disposition'] )

                if filename:
                    # Save the download
                    with open( os.path.join( target_dir, filename ), 'wb' ) as file:
                        for chunk in response.iter_content( chunk_size=8192 ):  # Adjust chunk_size as needed
                            file.write( chunk )

                    # Report success
                    print( '{: >3d}: {}'.format( count, filename ) )

                else:
                    # Report error
                    print( '  Error: {}'.format( 'Filename not found in content disposition header' ) )

            except requests.exceptions.RequestException as e:
                # Report error
                print( '{: >3d}: {}'.format( count, filename ) )
                print( '  Error: {}'.format( e ) )
                exit()

    if count == 0:
        print( '  No files found' )


# Extract filename from content disposition header
def get_filename( content_disposition ):

    filename = None

    filename_label = 'filename='

    if content_disposition:

        # Split disposition into parts
        parts = content_disposition.split(";")

        # File filename part
        for part in parts:

            part = part.strip()
            if part.startswith( filename_label ):
                filename = part[len( filename_label ):].strip( '"' )
                break

    return filename


######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Download MA docket filings' )
    parser.add_argument( '-n', dest='docket_number',  help='Docket number', required=True )
    parser.add_argument( '-d', dest='date',  help='Date of filing', required=True )
    parser.add_argument( '-f', dest='filer',  help='Filer', required=True )
    parser.add_argument( '-t', dest='target_directory', default=os.getcwd(), help='Target directory where downloads will be saved' )
    args = parser.parse_args()

    # Report start time
    print( '' )
    print( 'Starting at', time.strftime( '%H:%M:%S', time.localtime( START_TIME ) ) )

    # Get the Chrome driver
    driver = get_driver( args.target_directory )

    # Get the web page for the specified docket
    get_docket( driver, args.docket_number )

    # Get IDs of rows specified by date and filer
    ls_row_ids = get_row_ids( driver, args.date, args.filer )

    # Download files listed in identified rows
    download_files( driver, ls_row_ids, args.target_directory )

    # Close the browser
    driver.quit()

    # Report elapsed time
    report_elapsed_time()
