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

    #
    # Read data sources
    #
    print( '\n=======> Raw Electric Usage' )
    os.system( 'python xl_to_db.py -d ../xl/mass_save/electric_usage -l year -r 2 -n "Annual" -o ../db/{0} -t RawElectricUsage -c'.format( args.output_filename ) )

    print( '\n=======> Raw Gas Usage' )
    os.system( 'python xl_to_db.py -d ../xl/mass_save/gas_usage -l year -r 2 -n "Annual" -o ../db/{0} -t RawGasUsage'.format( args.output_filename ) )

    print( '\n=======> Raw Geographic Report' )
    os.system( 'python xl_to_db.py -d ../xl/mass_save/geographic_report -l year -r 1 -n "Gas Incentives" -o ../db/{0} -t RawGeographicReport'.format( args.output_filename ) )

    print( '\n=======> Raw Electric EES Rates' )
    os.system( 'python xl_to_db.py -i ../xl/mass_save/electric_ees_rates.xlsx -o ../db/{0} -t RawElectricEesRates -n "Electric Utility" -s "Year,Electric Utility"'.format( args.output_filename ) )

    print( '\n=======> Raw Gas EES Rates' )
    os.system( 'python xl_to_db.py -i ../xl/mass_save/gas_ees_rates.xlsx -o ../db/{0} -t RawGasEesRates -n "Gas Utility" -s "Year,Gas Utility"'.format( args.output_filename ) )

    #
    # Refine raw tables
    #
    common_columns = 'year,jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec,'

    print( '\n=======> Electric Usage' )
    numeric_columns = common_columns + 'annual_electric_usage_mwh'
    os.system( 'python mass_save_refine.py -i RawElectricUsage -o ElectricUsage -n {0} -d ../db/{1}'.format( numeric_columns, args.output_filename ) )

    print( '\n=======> Gas Usage' )
    numeric_columns = common_columns + 'annual_gas_usage_therms'
    os.system( 'python mass_save_refine.py -i RawGasUsage -o GasUsage -n {0} -d ../db/{1}'.format( numeric_columns, args.output_filename ) )

    print( '\n=======> Geographic Report' )
    numeric_columns = 'year,annual_electric_usage_mwh,annual_electric_savings_mwh,electric_incentives_$,annual_gas_usage_therms,annual_gas_savings_therms,gas_incentives_$'
    os.system( 'python mass_save_refine.py -i RawGeographicReport -o GeographicReport -r zip_code -n {0} -z "No gas" -d ../db/{1}'.format( numeric_columns, args.output_filename ) )

    print( '\n=======> Electric EES Rates' )
    numeric_columns = 'year'
    os.system( 'python mass_save_refine.py -i RawElectricEesRates -x -o ElectricEesRates -n {0} -d ../db/{1}'.format( numeric_columns, args.output_filename ) )

    print( '\n=======> Gas EES Rates' )
    numeric_columns = 'year'
    os.system( 'python mass_save_refine.py -i RawGasEesRates -x -o GasEesRates -n {0} -d ../db/{1}'.format( numeric_columns, args.output_filename ) )

    #
    # Create table of towns
    #
    print( '\n=======> Towns' )
    os.system( 'python mass_save_towns.py -d ../db/{0} -p ../xl/mass_save/population_2020.xlsx -e ../xl/mass_save/poverty_rates.xlsx -u ../xl/mass_save/electric_utilities.xlsx -v ../xl/mass_save/gas_utilities.xlsx'.format( args.output_filename ) )

    #
    # Analyze MassSave data
    #
    print( '\n=======> Analyze' )
    os.system( 'python mass_save_analyze.py -d ../db/{0}'.format( args.output_filename ) )

    print( '\n=======> Summarize' )
    os.system( 'python mass_save_summarize.py -s "Residential & Low-Income" -t SummaryResidential -d ../db/{0}'.format( args.output_filename ) )
    os.system( 'python mass_save_summarize.py -s "Commercial & Industrial" -t SummaryCommercial -d ../db/{0}'.format( args.output_filename ) )
    os.system( 'python mass_save_summarize.py -s "Total" -t SummaryTotal -d ../db/{0}'.format( args.output_filename ) )

    #
    # Publish database
    #
    print( '\n=======> Publish' )
    os.system( 'python mass_save_publish.py -i ../db/{0} -o ../db'.format( args.output_filename ) )


    util.report_elapsed_time()
