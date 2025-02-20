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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


# --> Time reporting -->
import time
START_TIME = time.time()

def report_elapsed_time( prefix='\n', start_time=START_TIME ):
    elapsed_time = round( ( time.time() - start_time ) * 1000 ) / 1000
    minutes, seconds = divmod( elapsed_time, 60 )
    ms = round( ( elapsed_time - int( elapsed_time ) ) * 1000 )
    print( prefix + 'Elapsed time: {:02d}:{:02d}.{:d}'.format( int( minutes ), int( seconds ), ms ) )
# <-- Time reporting <--


# Get ID of row with specified date and filer
def get_row_id( driver, s_date, s_filer ):

    # Wait for the page to fully load JavaScript-rendered content
    wait_parts = \
    [
        '//div[@class="divGridRow"]',
        '/div[@class="left"]',
        '[.//text()[contains(., "' + s_date + '" )]]',
        '/parent::div',
        '/div[@class="right"]',
        '[.//text()[contains(., "' + s_filer + '" )]]',
        '/parent::div',
    ]

    xpath = ''.join( wait_parts )

    print( '' )
    print( 'Waiting for requested filings...' )
    print( '  Date: {}'.format( s_date ) )
    print( '  Filer: {}'.format( s_filer ) )

    element = WebDriverWait( driver, 120 ).until( EC.presence_of_element_located( ( By.XPATH, xpath ) ) )
    row_id = element.get_attribute( 'id' )

    return row_id


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


# Download files to specified target directory
def download_files( driver, row_id, target_dir ):

    # Get list of download links
    download_parts = \
    [
        '//div[@id="files_' + row_id + '"]',
        '/a',
    ]

    xpath = ''.join( download_parts )

    links = driver.find_elements( By.XPATH, xpath )

    # Report download operation
    print( '' )
    print( 'Downloading {} files to {}:'.format( len( links ), target_dir ) )

    # Iterate over list of links
    count = 0
    for link in links:

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



######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Download MA docket filings' )
    parser.add_argument( '-n', dest='docket_number',  help='Docket number', required=True )
    parser.add_argument( '-d', dest='date',  help='Date of filing', required=True )
    parser.add_argument( '-f', dest='filer',  help='Filer', required=True )
    parser.add_argument( '-t', dest='target_directory',  help='Target directory where downloads will be saved' )
    args = parser.parse_args()

    # Report start time
    print( '' )
    print( 'Starting at', time.strftime( '%H:%M:%S', time.localtime( START_TIME ) ) )

    # Set target directory
    target_dir = os.getcwd() if ( args.target_directory == None ) else args.target_directory

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

    # Open the web page
    docket_url = 'https://eeaonline.eea.state.ma.us/DPU/Fileroom/dockets/bynumber/' + args.docket_number
    driver.get( docket_url )

    # Get ID of row specified by date and filer
    row_id = get_row_id( driver, args.date, args.filer )

    # Download files listed in identified row
    download_files( driver, row_id, target_dir )

    # Close the browser
    driver.quit()

    # Report elapsed time
    report_elapsed_time()
