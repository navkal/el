# Copyright 2025 Energize Andover.  All rights reserved.

import os

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

import argparse



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

    print( '----------' )
    print( 'Waiting to load: {}'.format( xpath ) )
    print( '----------' )

    element = WebDriverWait( driver, 120 ).until( EC.presence_of_element_located( ( By.XPATH, xpath ) ) )
    row_id = element.get_attribute( 'id' )

    return row_id


# Download files to specified target directory
def download_files( driver, row_id, target_dir ):

    download_parts = \
    [
        '//div[@id="files_' + row_id + '"]',
        '/a',
    ]

    xpath = ''.join( download_parts )
    print( xpath )

    links = driver.find_elements( By.XPATH, xpath )
    print( '' )
    print( 'Downloading {} files to {}:'.format( len( links ), target_dir ) )

    count = 0
    for link in links:
        try:
            count += 1
            print( '{: >3d}:  {}'.format( count, link.text ) )
            link.click()
        except Exception as e:
            print( '  Download failed: {}'.format( e ) )


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

    print( '--> GOT row_id', row_id )

    # Download files listed in identified row
    download_files( driver, row_id, target_dir )

    # Close the browser
    driver.quit()

