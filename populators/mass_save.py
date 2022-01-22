# Copyright 2022 Energize Andover.  All rights reserved.

import argparse
import os

import sys
sys.path.append( '../util' )
import util


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Process MassSave data' )
    parser.add_argument( '-o', dest='output_filename',  help='Output filename - Name of SQLite database file', required=True )
    args = parser.parse_args()

    # Read data sources
    print( '\n=======> Raw Electric Usage' )
    os.system( 'python xl_to_db.py -d ../xl/mass_save/electric_usage -l year -r 2 -n "Annual" -o ../db/{0} -t RawElectricUsage -c'.format( args.output_filename ) )
    print( '\n=======> Raw Gas Usage' )
    os.system( 'python xl_to_db.py -d ../xl/mass_save/gas_usage -l year -r 2 -n "Annual" -o ../db/{0} -t RawGasUsage'.format( args.output_filename ) )
    print( '\n=======> Raw Geographic Report' )
    os.system( 'python xl_to_db.py -d ../xl/mass_save/geographic_report -l year -r 1 -n "Gas Incentives" -o ../db/{0} -t RawGeographicReport'.format( args.output_filename ) )

    # Clean raw data
    month_columns = 'jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec,'
    print( '\n=======> Electric Usage' )
    numeric_columns = month_columns + 'annual_electric_usage_mwh'
    os.system( 'python energy_usage.py -i RawElectricUsage -o ElectricUsage -n {0} -d ../db/{1}'.format( numeric_columns, args.output_filename ) )
    print( '\n=======> Gas Usage' )
    numeric_columns = month_columns + 'annual_gas_usage_therms'
    os.system( 'python energy_usage.py -i RawGasUsage -o GasUsage -n {0} -d ../db/{1}'.format( numeric_columns, args.output_filename ) )
    print( '\n=======> Geographic Report' )
    numeric_columns = 'annual_electric_usage_mwh,annual_electric_savings_mwh,electric_incentives_$,annual_gas_usage_therms,annual_gas_savings_therms,gas_incentives_$'
    os.system( 'python energy_usage.py -i RawGeographicReport -o GeographicReport -r zip_code -n {0} -d ../db/{1}'.format( numeric_columns, args.output_filename ) )

    util.report_elapsed_time()
