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
    os.system( 'python xl_to_db.py -d ../xl/mass_save/electric_usage -l year -r 2 -n "Annual" -o {0} -t RawElectricUsage -c'.format( args.output_filename ) )

    print( '\n=======> Raw Gas Usage' )
    os.system( 'python xl_to_db.py -d ../xl/mass_save/gas_usage -l year -r 2 -n "Annual" -o {0} -t RawGasUsage'.format( args.output_filename ) )

    print( '\n=======> Raw Geographic Report' )
    os.system( 'python xl_to_db.py -d ../xl/mass_save/geographic_report -l year -r 1 -n "Gas Incentives" -o {0} -t RawGeographicReport'.format( args.output_filename ) )

    print( '\n=======> Electric EES Rates' )
    os.system( 'python xl_to_db.py -i ../xl/mass_save/electric_ees_rates.xlsx -o {0} -t ElectricEesRates -n "Electric Utility" -s "Year,Electric Utility"'.format( args.output_filename ) )

    print( '\n=======> Gas EES Rates' )
    os.system( 'python xl_to_db.py -i ../xl/mass_save/gas_ees_rates.xlsx -o {0} -t GasEesRates -n "Gas Utility" -s "Year,Gas Utility"'.format( args.output_filename ) )

    #
    # Refine raw tables
    #
    common_columns = 'jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec,'

    print( '\n=======> Electric Usage' )
    numeric_columns = common_columns + 'annual_electric_usage_mwh'
    os.system( 'python mass_save_refine.py -i RawElectricUsage -o ElectricUsage -n {0} -d {1}'.format( numeric_columns, args.output_filename ) )

    print( '\n=======> Gas Usage' )
    numeric_columns = common_columns + 'annual_gas_usage_therms'
    os.system( 'python mass_save_refine.py -i RawGasUsage -o GasUsage -n {0} -d {1}'.format( numeric_columns, args.output_filename ) )

    print( '\n=======> Geographic Report' )
    numeric_columns = 'annual_electric_usage_mwh,annual_electric_savings_mwh,electric_incentives_$,annual_gas_usage_therms,annual_gas_savings_therms,gas_incentives_$'
    os.system( 'python mass_save_refine.py -i RawGeographicReport -o GeographicReport -r zip_code -n {0} -z "No gas,Municipal" -p "Protected" -c GeographicReportDropped -d {1}'.format( numeric_columns, args.output_filename ) )

    #
    # Create table of towns
    #
    print( '\n=======> Towns' )
    os.system( 'python mass_save_towns.py -d {0} -p ../xl/mass_save/population_2020.xlsx -e ../xl/mass_save/poverty_rates.xlsx -u ../xl/mass_save/electric_utilities.xlsx -v ../xl/mass_save/gas_utilities.xlsx'.format( args.output_filename ) )

    #
    # Analyze MassSave data
    #
    print( '\n=======> Analyze' )
    os.system( 'python mass_save_analyze.py -d {0}'.format( args.output_filename ) )

    #
    # Summarize MassSave data
    #
    print( '\n=======> Summarize Residential' )
    os.system( 'python mass_save_summarize.py -s "Residential & Low-Income" -t SummaryResidential -d {0}'.format( args.output_filename ) )

    print( '\n=======> Summarize Commercial' )
    os.system( 'python mass_save_summarize.py -s "Commercial & Industrial" -t SummaryCommercial -d {0}'.format( args.output_filename ) )

    print( '\n=======> Summarize Total' )
    os.system( 'python mass_save_summarize.py -s "Total" -t SummaryTotal -d {0}'.format( args.output_filename ) )

    #
    # Calculate statistics
    #
    print( '\n=======> Cost per Saved Therm' )
    os.system( 'python mass_save_cost_per_saved_therm.py -t CostPerSavedTherm -d {0}'.format( args.output_filename ) )

    print( '\n=======> Cost per Saved MWh' )
    os.system( 'python mass_save_cost_per_saved_mwh.py -t CostPerSavedMwh -d {0}'.format( args.output_filename ) )

    #
    # Semiannual Reports
    #
    print( '\n=======> Equity Zip Codes' )
    os.system( 'python xl_to_db.py -i ../xl/mass_save/equity_zip_codes.xlsx -z -t EquityZipCodes -o {0}'.format( args.output_filename ) )

    print( '\n=======> Raw Semiannual Reports' )
    os.system( 'python xl_to_db.py -d ../xl/mass_save/semiannual_report -a "Wxn & HPs by Zip" -r 6 -m -l year,quarter -t RawSemiannualReport -o {0}'.format( args.output_filename ) )

    print( '\n=======> Analyze Semiannual Reports' )
    os.system( 'python mass_save_semiannual_report.py -d {0}'.format( args.output_filename ) )

    #
    # Participation Reports
    #

    s_sector = util.SECTOR_COM_AND_IND.split()[0].lower()
    s_sector_cap = s_sector.capitalize()
    s_fuel = util.ELECTRIC.lower()
    s_fuel_cap = s_fuel.capitalize()
    s_dir = f'/{s_sector}/{s_fuel}'
    s_table = f'RawParticipation{s_sector_cap}{s_fuel_cap}'

    print( f'\n=======> Participation Report: {s_sector_cap} {s_fuel_cap}' )
    os.system( 'python xl_to_db.py -d ../xl/mass_save/participation_report{0} -l year -t {1} -o {2}'.format( s_dir, s_table, args.output_filename ) )

    s_fuel = util.GAS.lower()
    s_fuel_cap = s_fuel.capitalize()
    s_dir = f'/{s_sector}/{s_fuel}'
    s_table = f'RawParticipation{s_sector_cap}{s_fuel_cap}'

    print( f'\n=======> Participation Report: {s_sector_cap} {s_fuel_cap}' )
    os.system( 'python xl_to_db.py -d ../xl/mass_save/participation_report{0} -l year -t {1} -o {2}'.format( s_dir, s_table, args.output_filename ) )

    s_sector = util.SECTOR_RES_AND_LOW.split()[0].lower()
    s_sector_cap = s_sector.capitalize()
    s_fuel = util.ELECTRIC.lower()
    s_fuel_cap = s_fuel.capitalize()
    s_dir = f'/{s_sector}/{s_fuel}'
    s_table = f'RawParticipation{s_sector_cap}{s_fuel_cap}'

    print( f'\n=======> Participation Report: {s_sector_cap} {s_fuel_cap}' )
    os.system( 'python xl_to_db.py -d ../xl/mass_save/participation_report{0} -l year -t {1} -o {2}'.format( s_dir, s_table, args.output_filename ) )

    s_fuel = util.GAS.lower()
    s_fuel_cap = s_fuel.capitalize()
    s_dir = f'/{s_sector}/{s_fuel}'
    s_table = f'RawParticipation{s_sector_cap}{s_fuel_cap}'

    print( f'\n=======> Participation Report: {s_sector_cap} {s_fuel_cap}' )
    os.system( 'python xl_to_db.py -d ../xl/mass_save/participation_report{0} -l year -t {1} -o {2}'.format( s_dir, s_table, args.output_filename ) )

    print( '\n=======> Analyze Participation Reports' )
    os.system( 'python mass_save_participation_report.py -d {0}'.format( args.output_filename ) )


    # Generate copyright notice
    print( '\n=======> Copyright' )
    util.create_about_table( 'MassSave', util.make_df_about_energize_lawrence(), args.output_filename )

    #
    # Publish database
    #
    print( '\n=======> Publish' )
    os.system( 'python mass_save_publish.py -i {0} -o ../db'.format( args.output_filename ) )


    util.report_elapsed_time()
