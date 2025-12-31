# Copyright 2019 Energize Andover.  All rights reserved.

import argparse
import os

import sys
sys.path.append( '../util' )
import util


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Process election data' )
    parser.add_argument( '-o', dest='output_filename',  help='Output filename - Name of SQLite database file', required=True )
    parser.add_argument( '-d', dest='debug', action='store_true', help='Include debug columns in lookup table?' )
    args = parser.parse_args()

    # Read data sources
    print( '\n=======> Elections 2012-2019' )
    os.system( 'python elections.py -i ../xl/elections_2012-2019.xlsx -o ../db/{0} -c'.format( args.output_filename ) )
    print( '\n=======> Town Meetings 2009-2019' )
    os.system( 'python elections.py -i ../xl/town_meetings_2009-2019.xlsx -o ../db/{0} -e TM'.format( args.output_filename ) )
    print( '\n=======> Presidential Primary 2020' )
    os.system( 'python elections.py -i ../xl/presidential_primary_2020.xlsx -o ../db/{0} -e PP'.format( args.output_filename ) )
    print( '\n=======> Census' )
    os.system( 'python xl_to_db.py -i ../xl/census_2019-06.xlsx -o ../db/{0} -t Census'.format( args.output_filename ) )
    print( '\n=======> Gender_2014' )
    os.system( 'python xl_to_db.py -i ../xl/gender_2014.xlsx -o ../db/{0} -k "Resident Id Number,Gender" -t Gender_2014'.format( args.output_filename ) )
    print( '\n=======> Assessment' )
    os.system( 'python xl_to_db.py -i ../xl/assessment_2025-12.xlsx -m -p "LUC.1" -q "TotalValue,0" -o ../db/{0} -t Assessment'.format( args.output_filename ) )
    print( '\n=======> Water' )
    os.system( 'python xl_to_db.py -d ../xl/water -l service_type -n acct_no -s name,number,cur_date -o ../db/{0} -t Water'.format( args.output_filename ) )
    print( '\n=======> Raw Local Election Results' )
    os.system( 'python xl_to_db.py -d ../xl/election_results/local -l election_date -r 1 -p TOTALS -o ../db/{0} -t RawLocalElectionResults'.format( args.output_filename ) )
    print( '\n=======> Solar' )
    os.system( 'python xl_to_db.py -i ../xl/solar_2014-02-28.xlsx -o ../db/{0} -t Solar'.format( args.output_filename ) )
    print( '\n=======> Polling Places' )
    os.system( 'python xl_to_db.py -i ../xl/polling_places_2012-2019.xlsx -o ../db/{0} -t PollingPlaces'.format( args.output_filename ) )
    print( '\n=======> Employees' )
    os.system( 'python xl_to_db.py -i ../xl/employees_2017.xlsx -o ../db/{0} -t Employees'.format( args.output_filename ) )
    print( '\n=======> Assessment Addendum' )
    os.system( 'python xl_to_db.py -i ../xl/assessment_addendum.xlsx -o ../db/{0} -t AssessmentAddendum'.format( args.output_filename ) )
    print( '\n=======> Building Permits' )
    os.system( 'python building_permits.py -d ../xl/building_permits -o ../db/{0} -t BuildingPermits'.format( args.output_filename ) )

    # Add value
    print( '\n=======> Lookup' )
    os.system( 'python lookup.py -m ../db/{0}'.format( args.output_filename ) + ( ' -d' if args.debug else '' ) )
    print( '\n=======> Election History' )
    os.system( 'python election_history.py -m ../db/{0}'.format( args.output_filename ) )
    print( '\n=======> Water Consumption' )
    os.system( 'python water_consumption.py -m ../db/{0}'.format( args.output_filename ) )
    print( '\n=======> Water Customers' )
    os.system( 'python water_consumption.py -m ../db/{0} -s'.format( args.output_filename ) )
    print( '\n=======> Local Election Results' )
    os.system( 'python local_election_results.py -m ../db/{0}'.format( args.output_filename ) )
    print( '\n=======> Residents' )
    os.system( 'python residents.py -m ../db/{0}'.format( args.output_filename ) + ( ' -d' if args.debug else '' ) )
    print( '\n=======> Partisans_D' )
    os.system( 'python partisans.py -m ../db/{0}'.format( args.output_filename ) + ' -p D' )
    print( '\n=======> Partisans_R' )
    os.system( 'python partisans.py -m ../db/{0}'.format( args.output_filename ) + ' -p R' )
    print( '\n=======> Streets' )
    os.system( 'python partitions.py -m ../db/{0} -p street_name -t Streets'.format( args.output_filename ) )
    print( '\n=======> Precincts' )
    os.system( 'python partitions.py -m ../db/{0} -p precinct_number -t Precincts'.format( args.output_filename ) )
    print( '\n=======> Zones' )
    os.system( 'python partitions.py -m ../db/{0} -p zoning_code_1 -t Zones'.format( args.output_filename ) )

    util.report_elapsed_time()
