# Copyright 2025 Energize Andover.  All rights reserved.

import argparse

import os
import requests

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


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
    print( 'Loading page...' )

    element = WebDriverWait( driver, 120 ).until( EC.presence_of_element_located( ( By.XPATH, xpath ) ) )
    row_id = element.get_attribute( 'id' )

    return row_id


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
        filename = link.text

        try:
            # Get the download
            response = requests.get( link.get_attribute( 'href' ), stream=True )

            # Raise HTTPError for bad response (4xx or 5xx)
            response.raise_for_status()

            # Try to fix filename if file extension is missing
            filename = fix_pdf_filename( response, filename )

            # Save the download
            with open( os.path.join( target_dir, filename ), 'wb' ) as file:
                for chunk in response.iter_content( chunk_size=8192 ):  # Adjust chunk_size as needed
                    file.write( chunk )

            # Report success
            print( '{: >3d}: {}'.format( count, filename ) )

        except requests.exceptions.RequestException as e:
            # Report error
            print( '{: >3d}: {}'.format( count, filename ) )
            print( '  Error: {}'.format( e ) )


# Fix filename if it is a PDF missing the proper extension
def fix_pdf_filename( response, filename ):

    # Find out what kind of file we have
    content_type = response.headers.get( 'content-type' )

    # If it's a PDF...
    if content_type == 'application/pdf':

        # Get file extension from filename
        file_ext = filename.split( '.' )[-1]

        # Optionally fix filename
        file_type = 'pdf'
        if file_ext != file_type:
            filename = filename + '.' + file_type

    return filename


######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Load Excel sheet into SQLite database' )
    parser.add_argument( '-d', dest='date',  help='Date of filing', required=True )
    parser.add_argument( '-f', dest='filer',  help='Filer', required=True )
    parser.add_argument( '-t', dest='target_directory',  help='Target directory for downloads (optional)' )
    args = parser.parse_args()

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
    driver.get( 'https://eeaonline.eea.state.ma.us/DPU/Fileroom/dockets/bynumber/24-141' )

    # Get ID of row specified by date and filer
    row_id = get_row_id( driver, args.date, args.filer )

    # Download files listed in identified row
    download_files( driver, row_id, target_dir )

    # Close the browser
    driver.quit()

