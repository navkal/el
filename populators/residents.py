# Copyright 2019 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
import datetime
import time
from collections import OrderedDict

import gender_guesser.detector as gender_detector

import sys
sys.path.append( '../util' )
import util
import printctl


pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

#------------------------------------------------------


####################
# Global constants #
####################

GD = gender_detector.Detector()
GENDER_NA = ''
GENDER_M = 'M'
GENDER_F = 'F'


#------------------------------------------------------

###############
# Subroutines #
###############



def guess_gender( row ):

    if row[util.GENDER] == GENDER_NA:
        # Gender not in 2014 census.  Take a guess.

        guess = GD.get_gender( row[util.FIRST_NAME].capitalize() )

        if 'female' in guess:
            guess = GENDER_F
        elif 'male' in guess:
            guess = GENDER_M
        else:
            guess = GENDER_NA

    else:
        # Gender already known
        guess = row[util.GENDER]

    return guess


def report_gender_findings():

    vc = df_residents[util.GENDER].value_counts()
    pct_mf = int( 100 * ( vc.loc[GENDER_F] + vc.loc[GENDER_M] ) / len( df_census ) )
    print( 'Gender found for {0}% of residents'.format( pct_mf ) )

    df_voted = df_residents[ df_residents[util.VOTED] == util.YES ]
    vc = df_voted[util.GENDER].value_counts()
    pct_mf = int( 100 * ( vc.loc[GENDER_F] + vc.loc[GENDER_M] ) / len( df_voted ) )
    print( 'Gender found for {0}% of voters'.format( pct_mf ) )


def calculate_age( row ):

    birth_year = row[util.DATE_OF_BIRTH].split( '-' )[0]
    age = datetime.date.today().year - int( birth_year )

    return age


def get_zoning_code( row ):

    # If zoning code not already present...
    if pd.isnull( row[util.ZONING_CODE_1] ):

        # ... Look for it in the zone lookup table, ignoring the alternate street number
        zones_row = df_zones.loc[ ( df_zones[util.NORMALIZED_STREET_NUMBER] == row[util.NORMALIZED_STREET_NUMBER] ) & ( df_zones[util.NORMALIZED_STREET_NAME] == row[util.NORMALIZED_STREET_NAME] ) ]

        # Save result of lookup
        if len( zones_row ) == 0:
            code = ''
        else:
            code = zones_row[util.ZONING_CODE_1].values[0]
    else:
        # Zoning code already known; keep it
        code = row[util.ZONING_CODE_1]

    return code


def calculate_likely_dem_score( row ):
    party_affiliation = row[util.PARTY_AFFILIATION]
    score = likely_dem_score( row[util.PARTY_VOTED_HISTORY] ) if ( party_affiliation == util.U ) else likely_dem_score( party_affiliation * len( dc_primary_dates ) )
    return score


def likely_dem_score( s_history, scale=True ):

    score = 0
    increment = 1

    for c in s_history:

        if c == util.D:
            score += increment
        elif c == util.R:
            score -= increment

        increment += 0.1

    if scale:
        score = int( 100 * ( score / max_raw_dem_score ) )

    return score


def calculate_legacy_dem_score( row ):

    score = None

    # If current resident is young, unscored family member, generate legacy score
    if ( row[util.IS_FAMILY] == 1 ) and ( row[util.LIKELY_DEM_SCORE] == 0 ) and ( row[util.AGE] >= util.VOTER_AGE_MIN ) and ( row[util.AGE] <= util.LEGACY_AGE_MAX ) :

        # Find elders of this resident
        df_elders = df_residents[ ( df_residents[util.IS_HOMEOWNER] == 1 ) & ( df_residents[util.PARCEL_ID] == row[util.PARCEL_ID] ) & ( df_residents[util.AGE] > util.LEGACY_AGE_MAX ) ]

        # If elders found, assign legacy score
        if len( df_elders ) > 0:
            score = round( df_elders[util.LIKELY_DEM_SCORE].mean(), 1 )

    return score


def describe_primary_voter( df_rid ):

    # Count R and D votes
    vc = df_rid[util.PARTY_VOTED].value_counts()
    n_d = vc[util.D] if util.D in vc else 0
    n_r = vc[util.R] if util.R in vc else 0

    # Initialize row representing current resident
    voter_row = OrderedDict()

    # Add voting data to row
    voter_row[util.D] = n_d
    voter_row[util.R] = n_r
    voter_row[util.ABSENT] = len( dc_primary_dates ) - n_d - n_r
    voter_row[util.VOTED_BOTH] = util.YES if ( ( n_d > 0 ) and ( n_r > 0 ) ) else util.NO
    party_affiliation = df_rid.iloc[0][util.PARTY_AFFILIATION]
    voter_row[util.CHANGED_AFFILIATION] = util.YES if ( ( ( party_affiliation == util.R ) and ( n_d > 0 ) ) or ( ( party_affiliation == util.D ) and ( n_r > 0 ) ) ) else util.NO

    # Save voting history for current resident
    dates = dc_primary_dates.copy()
    for index, election_row in df_rid.iterrows():
        dates[ election_row[util.ELECTION_DATE].split()[0] ] = election_row[util.PARTY_VOTED]
    voter_row[util.PARTY_VOTED_HISTORY] = ''.join( dates.values() )
    voter_row.update( dates )

    df_voter_row = pd.DataFrame( [voter_row] )

    return df_voter_row


def filter_assessed_values( row ):
    assessed_value = row[util.TOTAL_ASSESSED_VALUE] if ( row[util.IS_HOMEOWNER] or row[util.IS_FAMILY] ) else None
    return assessed_value


def addYears( d, years ):
    try:
        return d.replace( year=d.year+years )
    except ValueError:
        # Handle February 29
        return d + ( datetime.date( d.year + years, 1, 1 ) - datetime.date( d.year, 1, 1 ) )


def calculate_voter_engagement_score( row ):

    if row[util.VOTED] == util.YES:
        # This resident has voted

        # List possible dates of earliest voting eligibility

        # Consider 18th birthday
        date_format = '%Y-%m-%d %H:%M:%S'
        age_18_date = addYears( datetime.datetime.strptime( row[util.DATE_OF_BIRTH], date_format ), 18 ).strftime( date_format )

        ls_dates = [ age_18_date ]

        # Consider most recent property transfer
        if not pd.isnull( row[util.LEGAL_REFERENCE_SALE_DATE] ):
            if row[util.NAL_DESCRIPTION] not in util.NAL_IGNORE:
                ls_dates.append( row[util.LEGAL_REFERENCE_SALE_DATE] )
            else:
                # Consider previous property transfer
                if not pd.isnull( row[util.PREVIOUS_LEGAL_REFERENCE_SALE_DATE] ):
                    if row[util.PREVIOUS_NAL_DESCRIPTION] not in util.NAL_IGNORE:
                        ls_dates.append( row[util.PREVIOUS_LEGAL_REFERENCE_SALE_DATE] )

        # Try dates in reverse order, until one produces a score within range
        ls_dates.sort( reverse=True )
        for s_date in ls_dates:
            df_eligible = df_election_history.loc[ df_election_history[util.ELECTION_DATE] >= s_date ]
            score = round( 100 * df_scores.loc[ row[util.RESIDENT_ID] ][util.SCORE] / df_eligible[util.SCORE].sum(), 2 )
            if score <= 100:
                break

    else:
        # This resident has not voted
        score = 0

    return score


# Mark resident participation in general elections
def mark_who_voted_when():

    global df_residents

    # Create dataframe of general elections, sorted by date
    df_general = df_e2.copy()
    df_general = df_general[ df_general[util.ELECTION_TYPE] != util.ELECTION_TYPES['LOCAL_TOWN_MEETING']]
    df_general = df_general.sort_values( by=[util.ELECTION_DATE] )

    # Iterate over general elections
    for idx, df_group in df_general.groupby( by=[util.ELECTION_DATE] ):

        # Create copy of current general election
        df_ge = df_group.copy()

        # Format label representing this election
        date = idx.split()[0]
        type = df_ge.iloc[0][util.ELECTION_TYPE].split()[0].lower()
        col = util.VOTED + '_' + type + '_' + date

        # Show who participated
        df_ge[col] = util.YES
        df_ge = df_ge[ [util.RESIDENT_ID, col] ]
        df_residents = pd.merge( df_residents, df_ge, how='left', on=[util.RESIDENT_ID] )

        # Show who didn't participate
        df_residents[col] = df_residents[col].fillna( util.NO )


def count_town_meeting_attendance():

    # Get town meeting attendance records
    df_town_meetings = df_e2[ df_e2[util.ELECTION_TYPE] == util.ELECTION_TYPES['LOCAL_TOWN_MEETING'] ]

    # Count attendance at all town meetings
    df_tm = df_town_meetings[util.RESIDENT_ID].value_counts().rename(util.TOWN_MEETINGS_ATTENDED).to_frame()
    df_tm[util.RESIDENT_ID] = df_tm.index

    # Isolate town meeting history
    df_meeting_history = df_election_history[ df_election_history[util.ELECTION_TYPE] == util.ELECTION_TYPES['LOCAL_TOWN_MEETING'] ].sort_values( by=[util.ELECTION_YEAR], ascending=[False] )

    # List recent town meeting dates, organized by year
    dc_recent_years = {}
    for idx, df_group in df_meeting_history.groupby( by=[util.ELECTION_YEAR], sort=False ):
        dc_recent_years[str(idx)] = df_group[util.ELECTION_DATE].tolist()
        if len( dc_recent_years ) >= util.RECENT_LOCAL_ELECTION_COUNT:
            break

    # Count attendance at recent town meetings
    for year in dc_recent_years.keys():
        col = util.TOWN_MEETINGS_ATTENDED + '_' + year
        df_tm_year = df_town_meetings[ df_town_meetings[util.ELECTION_DATE].isin( dc_recent_years[year] ) ]
        df_tm_year = df_tm_year[util.RESIDENT_ID].value_counts().rename(col).to_frame()
        df_tm_year[util.RESIDENT_ID] = df_tm_year.index
        df_tm = pd.merge( df_tm, df_tm_year, how='left', on=util.RESIDENT_ID )
        df_tm[col] = df_tm[col].fillna(0).astype(int)

    return df_tm


def make_gal_per_day_col_name( service_type ):
    return service_type.lower() + '_' + util.GAL_PER_DAY


# Mark resident participation in general elections
def add_water_data():

    global df_residents

    # Extract lookup rows that contain meter numbers
    df_meter_lookup = df_lookup[ df_lookup[util.METER_NUMBER] != '' ]

    # Read Water Customers table from database
    df_customers = pd.read_sql_table( 'WaterCustomers', engine )
    df_customers[util.METER_NUMBER] = df_customers[util.METER_NUMBER].astype(str)

    # Generate column names
    dc_serv_types = {}
    for serv_type in df_customers[util.SERVICE_TYPE].unique():
        dc_serv_types[serv_type] = make_gal_per_day_col_name( serv_type )

    # Iterate over residents
    for i, res_row in df_residents.iterrows():

        # Get next Resident ID
        res_id = res_row[util.RESIDENT_ID]

        # Find meters belonging to current resident
        df_meters = df_meter_lookup[ df_meter_lookup[util.RESIDENT_ID] == res_id ]

        # Process each meter
        for j, meter_row in df_meters.iterrows():

            # Get meter number
            meter_num = meter_row[util.METER_NUMBER]

            # Find water customer with this meter number
            df_customer = df_customers[ df_customers[util.METER_NUMBER] == meter_num ]

            # Save gallons-per-day measurement in corresponding column of Residents table
            if len( df_customer ) > 0:
                cust_row = df_customer.iloc[0]
                if cust_row[util.METER_STATUS] != util.METER_ANOMALY:
                    df_residents.at[ df_residents[util.RESIDENT_ID] == res_id, make_gal_per_day_col_name( cust_row[util.SERVICE_TYPE] )] = cust_row[util.GAL_PER_DAY]

                    if cust_row[util.GAL_PER_DAY] < 0:
                        print( cust_row )

    return



#------------------------------------------------------

###############
# Main script #
###############

if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Generate table listing and describing all residents' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    parser.add_argument( '-d', dest='debug', action='store_true', help='Include debug columns?' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Read census tables from database
    df_census = pd.read_sql_table( 'Census', engine, index_col=util.ID, columns=[util.ID, util.RESIDENT_ID, util.LAST_NAME, util.FIRST_NAME, util.DATE_OF_BIRTH, util.PARTY_AFFILIATION, util.OCCUPATION, util.RADDR_STREET_NUMBER_SUFFIX, util.RADDR_APARTMENT_NUMBER, util.PRECINCT_NUMBER] )
    df_census[util.FIRST_NAME] = df_census[util.FIRST_NAME].fillna( '' )
    df_census[util.PARTY_AFFILIATION] = df_census[util.PARTY_AFFILIATION].str.strip()
    df_gender = pd.read_sql_table( 'Gender_2014', engine, columns=[util.RESIDENT_ID,util.GENDER] )

    # Read election tables from database
    election_columns = [util.RESIDENT_ID, util.ELECTION_DATE, util.ELECTION_TYPE]
    df_e1 = pd.read_sql_table( 'ElectionModel_01', engine, columns=election_columns )
    df_e2 = pd.read_sql_table( 'ElectionModel_02', engine, columns=election_columns )
    df_e3 = pd.read_sql_table( 'ElectionModel_03', engine, columns=election_columns )

    # Combine all elections into one dataframe
    df_elections = df_e1.append( df_e2 ).append( df_e3 )

    # Build list of all voters
    df_all_voters = df_elections.copy().drop_duplicates( subset=[util.RESIDENT_ID] )
    df_all_voters = df_all_voters.drop( columns=[util.ELECTION_DATE, util.ELECTION_TYPE] )
    df_all_voters[util.VOTED] = util.YES

    # Count primary election votes
    df_pe = df_e1
    df_pe = df_pe[util.RESIDENT_ID].value_counts().rename(util.PRIMARY_ELECTIONS_VOTED).to_frame()
    df_pe[util.RESIDENT_ID] = df_pe.index

    # Count local election votes
    df_le = df_e2[ df_e2[util.ELECTION_TYPE] == util.ELECTION_TYPES['LOCAL_ELECTION'] ]
    df_le = df_le[util.RESIDENT_ID].value_counts().rename(util.LOCAL_ELECTIONS_VOTED).to_frame()
    df_le[util.RESIDENT_ID] = df_le.index

    # Count state election votes
    df_se = df_e2[ df_e2[util.ELECTION_TYPE] == util.ELECTION_TYPES['STATE_ELECTION'] ]
    df_se = df_se[util.RESIDENT_ID].value_counts().rename(util.STATE_ELECTIONS_VOTED).to_frame()
    df_se[util.RESIDENT_ID] = df_se.index

    # Count special election votes
    df_sp = df_e2[ df_e2[util.ELECTION_TYPE] == util.ELECTION_TYPES['SPECIAL_STATE'] ]
    df_sp = df_sp[util.RESIDENT_ID].value_counts().rename(util.SPECIAL_ELECTIONS_VOTED).to_frame()
    df_sp[util.RESIDENT_ID] = df_sp.index

    # Count attendance at town meetings
    df_election_history = pd.read_sql_table( 'ElectionHistory', engine, columns=[util.ELECTION_YEAR, util.ELECTION_DATE, util.ELECTION_TYPE, util.SCORE] )
    df_tm = count_town_meeting_attendance()

    # Count early votes
    df_ev = df_e3
    df_ev = df_ev[util.RESIDENT_ID].value_counts().rename(util.EARLY_VOTES).to_frame()
    df_ev[util.RESIDENT_ID] = df_ev.index

    # Read and combine Lookup and Assessment tables
    df_lookup = pd.read_sql_table( 'Lookup', engine, columns=[util.RESIDENT_ID, util.NORMALIZED_STREET_NUMBER, util.LADDR_ALT_STREET_NUMBER, util.NORMALIZED_STREET_NAME, util.PARCEL_ID, util.IS_HOMEOWNER, util.IS_FAMILY, util.METER_NUMBER] )
    df_lookup = df_lookup.dropna( subset=[util.RESIDENT_ID] )
    df_homes = df_lookup.copy()
    df_homes = df_homes.drop( columns=[util.METER_NUMBER] )
    df_homes = df_homes.drop_duplicates( subset=[util.RESIDENT_ID] )
    df_assessment = pd.read_sql_table( 'Assessment', engine, columns=[util.PARCEL_ID, util.LEGAL_REFERENCE_SALE_DATE, util.NAL_DESCRIPTION, util.PREVIOUS_LEGAL_REFERENCE_SALE_DATE, util.PREVIOUS_NAL_DESCRIPTION, util.TOTAL_ASSESSED_VALUE] )
    df_homes = pd.merge( df_homes, df_assessment, how='left', on=[util.PARCEL_ID] )
    df_homes[util.TOTAL_ASSESSED_VALUE] = df_homes.apply( lambda row: filter_assessed_values( row ), axis=1 )

    # Merge
    df_residents = pd.merge( df_census, df_all_voters, how='left', on=util.RESIDENT_ID )
    df_residents[util.VOTED] = df_residents[util.VOTED].fillna( util.NO )
    df_residents = pd.merge( df_residents, df_gender, how='left', on=util.RESIDENT_ID )
    df_residents[util.GENDER] = df_residents[util.GENDER].fillna( GENDER_NA )
    df_residents = pd.merge( df_residents, df_homes, how='left', on=util.RESIDENT_ID )
    df_residents[util.IS_HOMEOWNER] = df_residents[util.IS_HOMEOWNER].astype(int)
    df_residents[util.IS_FAMILY] = df_residents[util.IS_FAMILY].astype(int)
    df_residents = pd.merge( df_residents, df_pe, how='left', on=util.RESIDENT_ID )
    df_residents[util.PRIMARY_ELECTIONS_VOTED] = df_residents[util.PRIMARY_ELECTIONS_VOTED].fillna(0).astype(int)
    df_residents = pd.merge( df_residents, df_le, how='left', on=util.RESIDENT_ID )
    df_residents[util.LOCAL_ELECTIONS_VOTED] = df_residents[util.LOCAL_ELECTIONS_VOTED].fillna(0).astype(int)
    df_residents = pd.merge( df_residents, df_se, how='left', on=util.RESIDENT_ID )
    df_residents[util.STATE_ELECTIONS_VOTED] = df_residents[util.STATE_ELECTIONS_VOTED].fillna(0).astype(int)
    df_residents = pd.merge( df_residents, df_sp, how='left', on=util.RESIDENT_ID )
    df_residents[util.SPECIAL_ELECTIONS_VOTED] = df_residents[util.SPECIAL_ELECTIONS_VOTED].fillna(0).astype(int)
    df_residents = pd.merge( df_residents, df_tm, how='left', on=util.RESIDENT_ID )
    tm_cols = df_tm.columns.tolist()
    tm_cols.remove( util.RESIDENT_ID )
    for col in tm_cols:
        df_residents[col] = df_residents[col].fillna(0).astype(int)
    df_residents = pd.merge( df_residents, df_ev, how='left', on=util.RESIDENT_ID )
    df_residents[util.EARLY_VOTES] = df_residents[util.EARLY_VOTES].fillna(0).astype(int)

    # Guess gender where not known
    print( '' )
    report_gender_findings()
    print( 'Guessing missing genders...' )
    df_residents[util.GENDER] = df_residents.apply( lambda row: guess_gender( row ), axis=1 )
    report_gender_findings()
    print( '' )

    # Calculate age
    df_residents[util.AGE] = df_residents.apply( lambda row: calculate_age( row ), axis=1 )

    # Insert zoning codes
    t = time.time()
    print( 'Inserting zoning codes, starting at {0}...'.format( time.strftime( '%H:%M:%S', time.localtime( t ) ) ) )
    df_zones = pd.read_sql_table( 'ZoneLookup', engine, columns=[util.NORMALIZED_STREET_NUMBER, util.LADDR_ALT_STREET_NUMBER, util.NORMALIZED_STREET_NAME, util.ZONING_CODE_1] )
    df_residents = pd.merge( df_residents, df_zones, how='left', on=[util.NORMALIZED_STREET_NUMBER, util.LADDR_ALT_STREET_NUMBER, util.NORMALIZED_STREET_NAME] )
    df_residents[util.ZONING_CODE_1] = df_residents.apply( lambda row: get_zoning_code( row ), axis=1 )
    util.report_elapsed_time( 'Inserted zoning codes -- ', t )
    print( '' )

    # Load primary elections dataframe
    df_primary = pd.read_sql_table( 'ElectionModel_01', engine, parse_dates=True )
    df_primary = df_primary.sort_values( by=[util.ELECTION_DATE] )
    df_primary[util.PARTY_AFFILIATION] = df_primary[util.PARTY_AFFILIATION].str.strip()
    df_primary[util.PARTY_VOTED] = df_primary[util.PARTY_VOTED].str.strip()

    # Build dictionaries of unique primary election dates and corresponding column names
    dc_primary_dates = OrderedDict()
    dc_ballot_cols = {}
    for date in df_primary[util.ELECTION_DATE].unique():
        date_name = date.split()[0]
        dc_primary_dates[date_name] = '-'
        dc_ballot_cols[date_name] = 'primary_ballot_' + date_name

    # Calculate voter engagement score
    t = time.time()
    print( 'Calculating voter engagement scores, starting at {0}...'.format( time.strftime( '%H:%M:%S', time.localtime( t ) ) ) )
    df_scores = pd.merge( df_elections, df_election_history, how='left', on=[util.ELECTION_DATE, util.ELECTION_TYPE] )
    df_scores = df_scores.groupby( by=[util.RESIDENT_ID] ).sum()
    df_residents[util.VOTER_ENGAGEMENT_SCORE] = df_residents.apply( lambda row: calculate_voter_engagement_score( row ), axis=1 )
    util.report_elapsed_time( 'Scored voters -- ', t )
    print( '' )

    # Generate descriptions of primary voters
    t = time.time()
    print( 'Characterizing primary voters, starting at {0}...'.format( time.strftime( '%H:%M:%S', time.localtime( t ) ) ) )
    df_residents[util.PARTY_AFFILIATION] = df_residents[util.PARTY_AFFILIATION].fillna( '' )
    df_primary_voters = df_primary.groupby( by=[util.RESIDENT_ID] ).apply( describe_primary_voter )
    df_primary_voters = df_primary_voters.reset_index().drop( columns=['level_1'] )
    df_primary_voters = df_primary_voters.rename( columns=dc_ballot_cols )
    df_residents = pd.merge( df_residents, df_primary_voters, how='left', on=util.RESIDENT_ID )
    df_residents[util.PARTY_VOTED_HISTORY] = df_residents[util.PARTY_VOTED_HISTORY].fillna( '' )
    util.report_elapsed_time( 'Characterized primary voters -- ', t )

    # Calculate likely Democrat and party preference scores
    max_raw_dem_score = likely_dem_score( 'D' * len( dc_primary_dates ), scale=False )
    df_residents[util.LIKELY_DEM_SCORE] = df_residents.apply( lambda row: calculate_likely_dem_score( row ), axis=1 )
    df_residents[util.LEGACY_DEM_SCORE] = df_residents.apply( lambda row: calculate_legacy_dem_score( row ), axis=1 )
    df_residents[util.PARTY_PREFERENCE_SCORE] = df_residents.apply( lambda row: util.likely_dem_to_party_preference_score( row[util.LIKELY_DEM_SCORE] ), axis=1 )
    df_residents[util.LEGACY_PREFERENCE_SCORE] = df_residents.apply( lambda row: util.likely_dem_to_party_preference_score( row[util.LEGACY_DEM_SCORE] ), axis=1 )

    # Mark participation of residents in general elections
    mark_who_voted_when()

    # Add water consumption statistics
    add_water_data()

    # Sort
    df_residents = df_residents.sort_values( by=[util.NORMALIZED_STREET_NAME, util.NORMALIZED_STREET_NUMBER] )

    # Drop columns not wanted in database
    drop_columns = [
        util.DATE_OF_BIRTH,
        util.LEGAL_REFERENCE_SALE_DATE,
        util.NAL_DESCRIPTION,
        util.PREVIOUS_LEGAL_REFERENCE_SALE_DATE,
        util.PREVIOUS_NAL_DESCRIPTION
    ]
    df_residents = df_residents.drop( columns=drop_columns )

    # Optionally drop debug columns
    if not args.debug:
        debug_columns = [
        ]
        df_residents = df_residents.drop( columns=debug_columns )

    # Save result to database
    util.create_table( 'Residents', conn, cur, df=df_residents )

    # Report elapsed time
    util.report_elapsed_time()
