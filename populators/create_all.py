# Copyright 2019 Energize Andover.  All rights reserved.

import argparse
import os

import sys
sys.path.append( '../util' )
import util

# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Create all Andover databases' )
    parser.add_argument( '-d', dest='debug', action='store_true', help='Generate debug versions of databases?' )
    args = parser.parse_args()

    PYTHON = 'python '

    print( '\n=======> Master' )
    os.system( PYTHON + 'master.py -o ../db/master.sqlite' )

    print( '\n=======> Publish' )
    os.system( PYTHON + 'publish.py -i ../db/master.sqlite -o ../db' )

    if args.debug:

        print( '\n=======> Master Debug' )
        os.system( PYTHON + 'master.py -o ../db/master_debug.sqlite -d' )

        print( '\n=======> Publish Debug' )
        os.system( PYTHON + 'publish.py -i ../db/master_debug.sqlite -o ../db -d' )

    util.report_elapsed_time()
