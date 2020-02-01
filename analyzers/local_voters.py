# Copyright 2019 Energize Andover.  All rights reserved.

import sqlalchemy
import pandas as pd
from collections import OrderedDict
import datetime

import sys
sys.path.append( '../util' )
import util
import printctl


#------------------------------------------------------

####################
# Global constants #
####################

LABEL = 'label'
VALUE = 'value'


#------------------------------------------------------

###############
# Subroutines #
###############

def mark_local_elections_voted( df_group ):
    # Split date from full timestamp
    df_dates = df_group[util.ELECTION_DATE].str.split( expand=True )

    # Indicate that this resident voted on these dates
    df_dates[1] = util.YES

    # Set date as dataframe index
    df_dates = df_dates.set_index( 0 )

    # Make dates column heads
    df_dates = df_dates.transpose()

    # Delete name of columns index
    del df_dates.columns.name

    return df_dates




#------------------------------------------------------

###############
# Main script #
###############


# Open database
db_path = '../db/master.sqlite'
engine = sqlalchemy.create_engine( 'sqlite:///' + db_path )

# Read Residents table
resident_columns = \
[
    util.RESIDENT_ID,
    util.PRECINCT_NUMBER,
    util.LOCAL_ELECTIONS_VOTED,
    util.LIKELY_DEM_SCORE,
    util.VOTER_ENGAGEMENT_SCORE,
    util.PARTY_AFFILIATION,
    util.AGE,
    util.GENDER,
    util.OCCUPATION,
    util.TOTAL_ASSESSED_VALUE,
    util.IS_HOMEOWNER,
    util.IS_FAMILY,
    util.LAST_NAME,
    util.FIRST_NAME,
    util.NORMALIZED_STREET_NUMBER,
    util.NORMALIZED_STREET_NAME,
]
df_voters = pd.read_sql_table( 'Residents', engine, parse_dates=True, columns=resident_columns )

# Extract list of local voters
df_voters = df_voters[ df_voters[util.LOCAL_ELECTIONS_VOTED] > 0 ]


# Get records of local elections
election_columns = [util.RESIDENT_ID, util.ELECTION_DATE, util.ELECTION_TYPE]
df_local = pd.read_sql_table( 'ElectionModel_02', engine, columns=election_columns )
df_local = df_local[ df_local[util.ELECTION_TYPE] == util.ELECTION_TYPES['LOCAL_ELECTION'] ]
df_local = df_local.drop( columns=[util.ELECTION_TYPE] )

# Transpose local election data
df_local = df_local.groupby( by=[util.RESIDENT_ID] ).apply( mark_local_elections_voted )
df_local = df_local.reset_index()
df_local = df_local.drop( columns=['level_1'] )
df_local = df_local.sort_index( axis=1 )
df_local = df_local.fillna( util.NO )

# Merge local election data with voter data
df_voters = pd.merge( df_voters, df_local, how='left', on=[util.RESIDENT_ID] )

# Sort
df_voters = df_voters.sort_values( by=resident_columns[1:5], ascending=[True, False, False, False] )


# Generate summary
n_d_total = len( df_voters[df_voters[util.PARTY_AFFILIATION] == util.D] )
n_r_total = len( df_voters[df_voters[util.PARTY_AFFILIATION] == util.R] )
n_u_total = len( df_voters[df_voters[util.PARTY_AFFILIATION] == util.U] )

a_summary = []
a_summary.append( { LABEL: 'Local Voters', VALUE: len( df_voters ) } )
a_summary.append( { LABEL: 'Democratic Local Voters', VALUE: n_d_total } )
a_summary.append( { LABEL: 'Republican Local Voters', VALUE: n_r_total } )
a_summary.append( { LABEL: 'Unaffiliated Local Voters', VALUE: n_u_total } )
df_summary = pd.DataFrame( a_summary, columns=[ LABEL, VALUE ] )

# Save results to spreadsheets

# Create ID column for release version
df_voters[util.ID] = 1 + df_voters.reset_index().index
df_voters = df_voters.set_index( util.ID ).reset_index()

# Save debug and test versions of spreadsheet
with pd.ExcelWriter( '../analysis/local_voters_debug.xlsx' ) as writer:
    df_summary.to_excel( writer, sheet_name='Summary', index=False, header=False )
    df_voters.to_excel( writer, sheet_name='Local Voters', index=False )
df_voters.to_csv( '../test/local_voters.csv' )

# Remove debug columns
debug_columns = df_voters.columns
df_voters = df_voters.drop( columns=[util.RESIDENT_ID,util.FIRST_NAME,util.LAST_NAME] )
print( '' )
print( 'Columns removed for release version' )
print( list( debug_columns.difference( df_voters.columns ) ) )

# Save release version of spreadsheet
with pd.ExcelWriter( '../analysis/local_voters.xlsx' ) as writer:
    df_summary.to_excel( writer, sheet_name='Summary', index=False, header=False )
    df_voters.to_excel( writer, sheet_name='Local Voters', index=False )

util.report_elapsed_time()
