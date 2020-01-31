# Copyright 2019 Energize Andover.  All rights reserved.

import argparse
import os

import sys
sys.path.append( '../util' )
import util

# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Create all databases' )
    parser.add_argument( '-d', dest='debug', action='store_true', help='Generate debug versions of databases?' )
    args = parser.parse_args()

    PYTHON = 'python '
    XL_TO_DB = 'xl_to_db.py '
    CREATE = ' -c'

    print( '\n=======> Assessment' )
    os.system( PYTHON + XL_TO_DB + '-i ../xl/assessment_2019-06.xlsx -o ../db/assessment.sqlite -t Assessment' + CREATE )

    print( '\n=======> Census' )
    os.system( PYTHON + XL_TO_DB + '-i ../xl/census_2019-06.xlsx -o ../db/census.sqlite -t Census' + CREATE )

    print( '\n=======> Gender_2014' )
    os.system( PYTHON + XL_TO_DB + '-i ../xl/gender_2014.xlsx -o ../db/gender_2014.sqlite -k "Resident Id Number,Gender" -t Gender_2014' + CREATE )

    print( '\n=======> Polling Places' )
    os.system( PYTHON + XL_TO_DB + '-i ../xl/polling_places_2012-2019.xlsx -o ../db/polling_places.sqlite -t PollingPlaces' + CREATE )

    print( '\n=======> Employees' )
    os.system( PYTHON + XL_TO_DB + '-i ../xl/employees_2017.xlsx -o ../db/employees.sqlite -t Employees' + CREATE )

    print( '\n=======> Solar' )
    os.system( PYTHON + XL_TO_DB + '-i ../xl/solar_2014-02-28.xlsx -o ../db/solar.sqlite -t Solar' + CREATE )

    print( '\n=======> Water' )
    os.system( PYTHON + XL_TO_DB + '-d ../xl/water -l service_type -n acct_no -s name,number,cur_date -o ../db/water.sqlite -t Water' + CREATE )

    print( '\n=======> Raw Local Election Results' )
    os.system( PYTHON + XL_TO_DB + '-d ../xl/election_results/local -l election_date -r 1 -p TOTALS -o ../db/raw_local_election_results.sqlite -t RawLocalElectionResults' + CREATE )

    print( '\n=======> Assessment Addendum' )
    os.system( PYTHON + XL_TO_DB + '-i ../xl/assessment_addendum.xlsx -o ../db/assessment_addendum.sqlite -t AssessmentAddendum' + CREATE )

    print( '\n=======> Elections' )
    os.system( PYTHON + 'elections.py -i ../xl/elections_2012-2019.xlsx -o ../db/elections.sqlite' + CREATE )

    print( '\n=======> Town Meetings' )
    os.system( PYTHON + 'elections.py -i ../xl/town_meetings_2009-2019.xlsx -o ../db/town_meetings.sqlite -e TM' + CREATE )

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
