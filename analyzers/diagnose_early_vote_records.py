# Copyright 2019 Energize Andover.  All rights reserved.

import sqlalchemy
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util


#------------------------------------------------------

####################
# Global constants #
####################
BALLOT_DISP_1 = util.BALLOT_MAILED_DISPOSITION.format(1)
BALLOT_DISP_2 = util.BALLOT_MAILED_DISPOSITION.format(2)
BALLOT_DISP_3 = util.BALLOT_MAILED_DISPOSITION.format(3)
BALLOT_ACCEPTED = 'A'
BALLOT_REJECTED = 'R'

#------------------------------------------------------

###############
# Subroutines #
###############



#------------------------------------------------------

###############
# Main script #
###############


# Open database
db_path = '../db/master.sqlite'
engine = sqlalchemy.create_engine( 'sqlite:///' + db_path )

# Get records of early votes
early_columns = [util.RESIDENT_ID, util.ELECTION_DATE, BALLOT_DISP_1, BALLOT_DISP_2, BALLOT_DISP_3]
df_early = pd.read_sql_table( 'ElectionModel_03', engine, columns=early_columns )

# Exclude early votes that were not accepted
df_early = df_early[
        ( df_early[BALLOT_DISP_1] == BALLOT_ACCEPTED )
            |
        ( df_early[BALLOT_DISP_2] == BALLOT_ACCEPTED )
            |
        ( df_early[BALLOT_DISP_3] == BALLOT_ACCEPTED )
    ]

# Get records of all general elections
election_columns = [util.RESIDENT_ID, util.ELECTION_DATE, util.ELECTION_TYPE]
df_elections = pd.read_sql_table( 'ElectionModel_02', engine, columns=election_columns )

# Correlate early vote records with general election records
df = pd.merge( df_early, df_elections, how='left', on=[util.RESIDENT_ID, util.ELECTION_DATE] )

# Isolate early votes that are absent from general election records
df = df[ pd.isnull( df[util.ELECTION_TYPE] ) ]

# Drop columns no longer needed
df = df.drop( columns=[util.ELECTION_TYPE, BALLOT_DISP_1, BALLOT_DISP_2, BALLOT_DISP_3] )

# Load early voter names and addresses
res_columns = [util.RESIDENT_ID, util.LAST_NAME, util.FIRST_NAME, util.NORMALIZED_STREET_NUMBER, util.NORMALIZED_STREET_NAME]
df_residents = pd.read_sql_table( 'Residents', engine, columns=res_columns )
df = pd.merge( df, df_residents, how='left', on=[util.RESIDENT_ID] )

# df_non_res = df[ pd.isnull( df[util.LAST_NAME] ) & pd.isnull( df[util.FIRST_NAME] ) ]
# df_non_res = df_non_res.drop( columns=[util.LAST_NAME, util.FIRST_NAME, util.NORMALIZED_STREET_NUMBER, util.NORMALIZED_STREET_NAME] )
# df_non_res = df_non_res.sort_values( by=[util.RESIDENT_ID, util.ELECTION_DATE] )
# df_non_res = df_non_res.reset_index( drop=True )
# print( df_non_res )
# df_non_res.to_excel( '../analysis/uncounted_early_votes_not_in_census.xlsx', index=False )


# Drop voters with unknown names
df = df.dropna( subset=[util.LAST_NAME, util.FIRST_NAME], how='all' )

# Report findings
filename = '../analysis/early_votes_not_reported.xlsx'
print( '' )
print( 'Saving results to', filename )
print( '' )

with pd.ExcelWriter( filename ) as writer:
    for idx, df_group in df.groupby( by=[util.ELECTION_DATE] ):
        election_date = idx.split( ' ' )[0]
        print( 'Election date {0}: {1} early votes were not reported'.format( election_date, len( df_group ) ) )
        df_group.to_excel( writer, sheet_name=election_date, index=False )

util.report_elapsed_time()
