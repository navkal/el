# Copyright 2020 Energize Andover.  All rights reserved.

import argparse

import sys
sys.path.append( '../util' )
import util

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Correlate employees with residents' )
    parser.add_argument( '-i', dest='input_filename',  help='Input database filename' )
    parser.add_argument( '-v', dest='views_filename',  help='Filename of database containing views to be migrated' )
    parser.add_argument( '-o', dest='output_filename',  help='Output database filename' )
    args = parser.parse_args()

    util.copy_views( args.input_filename, args.views_filename, args.output_filename )
