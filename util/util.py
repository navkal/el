# Copyright 2019 Energize Andover.  All rights reserved.

import os
import sqlite3
import sqlalchemy
import pandas as pd
import string
from shutil import copyfile
import time
START_TIME = time.time()
print( 'Starting at', time.strftime( '%H:%M:%S', time.localtime( START_TIME ) ) )


ELECTION_TYPES = \
{
    'STATE_ELECTION': 'STATE ELECTION',
    'SPECIAL_STATE': 'SPECIAL STATE',
    'PRESIDENTIAL_PRIMARY': 'PRESIDENTIAL PRIMARY',
    'STATE_PRIMARY': 'STATE PRIMARY',
    'SPECIAL_STATE_PRIMARY': 'SPECIAL STATE PRIMARY',
    'LOCAL_ELECTION': 'LOCAL ELECTION',
    'LOCAL_TOWN_MEETING': 'LOCAL TOWN MEETING',
}

ID = 'id'

R = 'R'
D = 'D'
U = 'U'

ABSENT = '-'
VOTED_BOTH = 'voted_both'
CHANGED_AFFILIATION = 'changed_affiliation'
PARTY_VOTED_HISTORY = 'party_voted_history'
LIKELY_DEM_SCORE = 'likely_dem_score'
PARTY_PREFERENCE_SCORE = 'party_preference_score'
LEGACY_PREFERENCE_SCORE = 'legacy_preference_score'
LEGACY_DEM_SCORE = 'legacy_dem_score'
VOTER_ENGAGEMENT_SCORE = 'voter_engagement_score'
TOWN_MEETINGS_ATTENDED = 'town_meetings_attended'
PRIMARY_ELECTIONS_VOTED = 'primary_elections_voted'
LOCAL_ELECTIONS_VOTED = 'local_elections_voted'
STATE_ELECTIONS_VOTED = 'state_elections_voted'
SPECIAL_ELECTIONS_VOTED = 'special_elections_voted'
EARLY_VOTES = 'early_votes'
VOTED = 'voted'

PARTISAN_SCORE = 'partisan_score'
RECENT_LOCAL_ELECTION_COUNT = 3
RECENT_LOCAL_ELECTIONS_VOTED = 'recent_local_elections_voted'

ELECTION_YEAR = 'election_year'
TURNOUT = 'turnout'
TURNOUT_FACTOR = 'turnout_factor'
RECENCY_FACTOR = 'recency_factor'
SCORE = 'score'

YES = 'Y'
NO = 'N'

IS_HOMEOWNER = 'is_homeowner'
IS_FAMILY = 'is_family'
LEGACY_AGE_MAX = 30
VOTER_AGE_MIN = 18

CONVENIENCE = 'CONVENIENCE'
FAMILY = 'FAMILY'
NAL_IGNORE = [ CONVENIENCE, FAMILY ]

RESIDENT_COUNT = 'resident_count'
VOTER_COUNT = 'voter_count'
LIKELY_DEM_COUNT = 'likely_dem_count'
LIKELY_REPUB_COUNT = 'likely_repub_count'
LOCAL_DEM_VOTER_COUNT = 'local_dem_voter_count'
LOCAL_REPUB_VOTER_COUNT = 'local_repub_voter_count'

MEAN_LIKELY_DEM_SCORE = 'mean_likely_dem_score'
MEAN_PARTY_PREFERENCE_SCORE = 'mean_party_preference_score'
MEAN_VOTER_ENGAGEMENT_SCORE = 'mean_voter_engagement_score'
MEAN_TOTAL_ASSESSED_VALUE = 'mean_total_assessed_value'
MEAN_LIKELY_DEM_VOTER_ENGAGEMENT_SCORE = 'mean_likely_dem_voter_engagement_score'
MEAN_LIKELY_REPUB_VOTER_ENGAGEMENT_SCORE = 'mean_likely_repub_voter_engagement_score'

GAL_PER_CU_FT = 7.48052

METER_STATUS = 'meter_status'
METER_WRAP = 'wrap'
METER_NORMAL = 'normal'
METER_ANOMALY = 'anomaly'



# Mappings to reconcile column names

FIRST_NAME = 'first_name'
MIDDLE_NAME = 'middle_name'
LAST_NAME = 'last_name'
TITLE = 'title'
DATE_OF_BIRTH = 'date_of_birth'
ACCOUNT_NUMBER = 'account_number'
AGE = 'age'

PARTY_AFFILIATION = 'party_affiliation'
RESIDENT_ID = 'resident_id'
ELECTION_TYPE = 'election_type'
VOTER_STATUS = 'voter_status'
OCCUPATION = 'occupation'

STREET_NUMBER = '{0}_street_number'
STREET_NUMBER_SUFFIX = '{0}_street_number_suffix'
STREET_NAME = '{0}_street_name'
APARTMENT_NUMBER = '{0}_apartment_number'
ZIP_CODE = '{0}_zip_code'

NORMALIZED_ADDRESS = 'normalized_address'
NORMALIZED_STREET_NUMBER = 'street_number'
NORMALIZED_STREET_NAME = 'street_name'
ADDRESS = 'address'
ADDR_STREET_NUMBER = STREET_NUMBER.format( ADDRESS )
ADDR_STREET_NAME = STREET_NAME.format( ADDRESS )

LOCATION_ADDRESS = 'location_address'
LADDR_STREET_NUMBER = STREET_NUMBER.format( LOCATION_ADDRESS )
LADDR_ALT_STREET_NUMBER = STREET_NUMBER.format( LOCATION_ADDRESS + '_alternate' )
LADDR_STREET_NAME = STREET_NAME.format( LOCATION_ADDRESS )
LADDR_CITY = LOCATION_ADDRESS + '_city'
LADDR_CONDO_UNIT = LOCATION_ADDRESS + '_condo_unit'
LADDR_CONDO_COMPLEX = LOCATION_ADDRESS + '_condo_complex'

RESIDENTIAL_ADDRESS = 'residential_address'
RADDR_STREET_NUMBER = STREET_NUMBER.format( RESIDENTIAL_ADDRESS )
RADDR_STREET_NUMBER_SUFFIX = STREET_NUMBER_SUFFIX.format( RESIDENTIAL_ADDRESS )
RADDR_STREET_NAME = STREET_NAME.format( RESIDENTIAL_ADDRESS )
RADDR_APARTMENT_NUMBER = APARTMENT_NUMBER.format( RESIDENTIAL_ADDRESS )
RADDR_ZIP_CODE = ZIP_CODE.format( RESIDENTIAL_ADDRESS )
PRECINCT_NUMBER = 'precinct_number'

MAILING_ADDRESS = 'mailing_address'
MADDR_APARTMENT_NUMBER = APARTMENT_NUMBER.format( MAILING_ADDRESS )
MADDR_CITY = MAILING_ADDRESS + '_city'
MADDR_STATE = MAILING_ADDRESS + '_state'
MADDR_STREET = MAILING_ADDRESS + '_street'
MADDR_ZIP_CODE = ZIP_CODE.format( MAILING_ADDRESS )
MADDR_LINE = MAILING_ADDRESS + '_line_{0}'

TOWN_CODE = 'town_code'
TOWN_NAME = 'town_name'
TOWN_INDICATOR = 'town_indicator'

ELECTION_DATE = 'election_date'
PARTY_VOTED = 'party_voted'

BALLOT_MAILED_DISPOSITION = 'ballot_{0}_mailed_disposition'
BALLOT_RECEIVED_BY = 'ballot_{0}_received_by'
BALLOT_REJECTION_REASON = 'ballot_{0}_rejection_reason'
BALLOT_SENT_BY = 'ballot_{0}_sent_by'
BALLOT_MAILED_DATE = 'ballot_{0}_mailed_date'
BALLOT_RECEIVED_DATE = 'ballot_{0}_received_date'

PARCEL_ID = 'parcel_id'
PHONE = 'phone'
METER_NUMBER = 'meter_number'
SERVICE_ID = 'service_id'
DESCRIPTION = 'description'
CURRENT_DATE = 'current_date'
CURRENT_READING = 'current_reading'
PRIOR_DATE = 'prior_date'
PRIOR_READING = 'prior_reading'
TRANSACTION_DATE = 'transaction_date'
TRANSACTION_TYPE = 'transaction_type'
UNITS = 'units'
SERVICE_TYPE = 'service_type'
SERVICE = 'service'
CU_FT = 'cu_ft'
ELAPSED_DAYS = 'elapsed_days'
CU_FT_PER_DAY = 'cu_ft_per_day'
GAL_PER_DAY = 'gal_per_day'

OWNER_1_NAME = 'owner_1_name'
OWNER_2_NAME = 'owner_2_name'
OWNER_3_NAME = 'owner_3_name'

SITE_ADDRESS = 'site_address'

LEGAL_REFERENCE_SALE_DATE = 'legal_reference_sale_date'
NAL_DESCRIPTION = 'nal_description'
PREVIOUS_LEGAL_REFERENCE_SALE_DATE = 'previous_legal_reference_sale_date'
PREVIOUS_NAL_DESCRIPTION = 'previous_nal_description'
GRANTOR = 'grantor'
PREVIOUS_GRANTOR = 'previous_grantor'
OWNER_OCCUPIED = 'owner_occupied'
ZONING_CODE_1 = 'zoning_code_1'
TOTAL_ASSESSED_VALUE = 'total_assessed_value'

DEPARTMENT = 'department'
POSITION = 'position'

GENDER = 'gender'

OFFICE_OR_CANDIDATE = 'office_or_candidate'
OFFICE = 'office'
CANDIDATE = 'candidate'
PRECINCT = 'precinct_'
PRECINCT_1 = PRECINCT + '1'
PRECINCT_2 = PRECINCT + '2'
PRECINCT_3 = PRECINCT + '3'
PRECINCT_4 = PRECINCT + '4'
PRECINCT_5 = PRECINCT + '5'
PRECINCT_6 = PRECINCT + '6'
PRECINCT_7 = PRECINCT + '7'
PRECINCT_8 = PRECINCT + '8'
PRECINCT_9 = PRECINCT + '9'
TOTAL = 'total'

PIN = 'PIN'
PERMITFOR = 'PermitFor'
PERMIT_FOR = 'permit_for'
DATE_ISSUED = 'date_issued'
TOTALFEE = 'TotalFee'
PROJECTCOST = 'ProjectCost'
COST = 'Cost'


ZIP = 'zip_code'
ZIP_CODES = 'zip_codes'
YEAR = 'year'
COUNTY = 'county'
SECTOR = 'sector'

SECTOR_RES_AND_LOW = 'Residential & Low-Income'
SECTOR_COM_AND_IND = 'Commercial & Industrial'
SECTOR_TOTAL = 'Total'

COMBINED_EES_IN = 'combined_ees_in_$'
COMBINED_INCENTIVES_OUT = 'combined_incentives_out_$'
COMBINED_EES_MINUS_INCENTIVES = 'combined_ees_minus_incentives_$'

JAN = 'jan'
FEB = 'feb'
MAR = 'mar'
APR = 'apr'
MAY = 'may'
JUN = 'jun'
JUL = 'jul'
AUG = 'aug'
SEP = 'sep'
OCT = 'oct'
NOV = 'nov'
DEC = 'dec'

ANNUAL = 'annual'
ELECTRIC_USAGE = '_electric_usage_mwh'
GAS_USAGE = '_gas_usage_therms'
ELECTRIC_SAVINGS = '_electric_savings_mwh'
GAS_SAVINGS = '_gas_savings_therms'
ANNUAL_ELECTRIC_USAGE = ANNUAL + ELECTRIC_USAGE
ANNUAL_GAS_USAGE = ANNUAL + GAS_USAGE
ANNUAL_ELECTRIC_SAVINGS = ANNUAL + ELECTRIC_SAVINGS
ANNUAL_GAS_SAVINGS = ANNUAL + GAS_SAVINGS
ELECTRIC_INCENTIVES = 'electric_incentives_$'
GAS_INCENTIVES = 'gas_incentives_$'
POPULATION = 'population'
TRACT_POPULATION = 'tract_population'
PCT_LOW_INCOME = 'pct_low_income'
PCT_ENERGY_BURDENED = 'pct_energy_burdened'
RESIDENTIAL_DOL_PER_MWH = 'residential_$_per_mwh'
DISCOUNT_DOL_PER_MWH = 'discount_$_per_mwh'
COMMERCIAL_DOL_PER_MWH = 'commercial_$_per_mwh'
RESIDENTIAL_DOL_PER_THERM = 'residential_$_per_therm'
COMMERCIAL_DOL_PER_THERM = 'commercial_$_per_therm'
INCENTIVES_PER_SAVED_MWH = 'incentives_per_saved_mwh_$'
INCENTIVES_PER_SAVED_THERM = 'incentives_per_saved_therm_$'

ELECTRIC_UTILITY = 'electric_utility'
ELECTRIC_UTILITY_URL = 'electric_utility_url'

GAS_UTILITY = 'gas_utility'
GAS_UTILITY_1 = GAS_UTILITY + '_1'
GAS_UTILITY_2 = GAS_UTILITY + '_2'
GAS_UTILITY_URL_1 = GAS_UTILITY + '_url_1'
GAS_UTILITY_URL_2 = GAS_UTILITY + '_url_2'

RESIDENTIAL_RATE = 'residential_rate'
RESIDENTIAL_R1_RATE = 'residential_r1_rate'
RESIDENTIAL_R2_RATE = 'residential_r2_rate'
COMMERCIAL_RATE = 'commercial_rate'

CONSISTENT_COLUMN_NAMES = \
{
    'Assessment': \
    {
        'AccountNumber': ACCOUNT_NUMBER,
        'ParcelID': PARCEL_ID,
        'UserAccount': 'user_account',
        'Street.': LADDR_STREET_NUMBER,
        'AltStreet.': LADDR_ALT_STREET_NUMBER,
        'StreetName': LADDR_STREET_NAME,
        'LocCity': LADDR_CITY,
        'CondoUnit': LADDR_CONDO_UNIT,
        'CondoComplex': LADDR_CONDO_COMPLEX,
        'Owner1': OWNER_1_NAME,
        'Owner2': OWNER_2_NAME,
        'Owner3': OWNER_3_NAME,
        'BillingAddress': MADDR_LINE.format( 1 ),
        'BillingAddress2': MADDR_LINE.format( 2 ),
        'City': MADDR_CITY,
        'State': MADDR_STATE,
        'Zip': MADDR_ZIP_CODE,
        'OwnerOccupied': OWNER_OCCUPIED,
        'Zone1': ZONING_CODE_1,
        'FloodHazard': 'flood_hazard_zone',
        'Census': 'census_code',
        'Utility1': 'utility_code_1',
        'Utility2': 'utility_code_2',
        'Utility3': 'utility_code_3',
        'Traffic': 'traffic_code',
        'LUC': 'primary_land_use_code',
        'YearBuilt': 'year_built',
        'EffYr': 'effective_year_built',
        'TotalAcres': 'total_acres',
        'GrossBldArea': 'gross_building_area',
        'BuildType': 'building_type_1',
        'BuildType2': 'building_type_2',
        'BuildType3': 'building_type_3',
        'FinArea': 'finished_building_area',
        'NumofBuilding': 'number_of_buildings',
        'StoryHeight': 'story_height',
        'RentalLivUnits': 'rental_living_units',
        'Rooms': 'number_of_rooms',
        'Bedrooms': 'number_of_bedrooms',
        'Fullbaths': 'number_of_full_baths',
        'HalfBaths': 'number_of_half_baths',
        'OtherFixtures': 'number_of_other_fixtures',
        'BathRating': 'bathroom_rating',
        'Kitchens': 'number_of_kitchens',
        'KitchenRating': 'kitchen_rating',
        'FirePlaces': 'number_of_fireplaces',
        'WSFlues': 'number_of_wood_stove_flues',
        'SolarHotWater': 'solar_hot_water',
        'CentralVac': 'central_vacuum',
        'HeatType': 'heating_type',
        'HeatFuel': 'heating_fuel',
        'PercentAC': 'percent_air_conditioned',
        'BasementArea': 'basement_area',
        'FinBasementArea': 'finished_basement_area',
        'RoofType': 'roof_type',
        'RoofCover': 'roof_cover',
        'ExtWall': 'exterior_wall_type',
        'IntWall': 'interior_wall_type',
        'AttachedGarage': 'attached_garage_area',
        'DetachedGarage': 'detached_garage_size',
        'BasementGarage': 'number_of_basement_garages',
        'Pool': 'pool',
        'Frame': 'building_frame_type',
        'Floor': 'floor_type',
        'BaseYear': 'base_depreciation_year',
        'Grade': 'building_grade',
        'Cond': 'building_condition',
        'LegalReference': 'legal_reference_number',
        'LegalRefDate': LEGAL_REFERENCE_SALE_DATE,
        'SalePrice': 'sale_price',
        'NAL': NAL_DESCRIPTION,
        'Grantor': GRANTOR,
        'PrevLegalReference': 'previous_legal_reference_number',
        'PrevLegalRefDate': PREVIOUS_LEGAL_REFERENCE_SALE_DATE,
        'PrevSalePrice': 'previous_sale_price',
        'PrevNal': PREVIOUS_NAL_DESCRIPTION,
        'PrevGrantor': PREVIOUS_GRANTOR,
        'TotalLandValue': 'total_land_value',
        'TotalYardItemValue': 'total_yard_item_value',
        'TotalBuildingValue': 'total_building_value',
        'TotalValue': TOTAL_ASSESSED_VALUE,
        'LegalDescription': 'legal_description',
        'AdjArea': 'adjusted_area',
        'PercSprink': 'percolation_sprinkler',
        'RevDate': 'revision_date',
        'GisKey': 'gis_key',
    },
    'AssessmentAddendum': \
    {
        'Street.': LADDR_STREET_NUMBER,
        'AltStreet.': LADDR_ALT_STREET_NUMBER,
        'StreetName': LADDR_STREET_NAME,
        'Zone1': ZONING_CODE_1,
    },
    'BuildingPermits': \
    {
        PIN: 'pin',
        'OwnerName': 'owner_name',
        PERMITFOR: PERMIT_FOR,
        'DateIssued': DATE_ISSUED,
        'DateIssued/Submit': DATE_ISSUED,
        'DateIssued/': DATE_ISSUED,
        'ParcelID': PARCEL_ID,
        'House#': ADDR_STREET_NUMBER,
        'Street': ADDR_STREET_NAME,
        'OccupancyType': 'occupancy_type',
        'Occ.Type': 'occupancy_type',
        'Occ': 'occupancy_type',
        'OccType': 'occupancy_type',
        'Type': 'occupancy_type',
        'BuildingType': 'building_type',
        'BldgType': 'building_type',
        'WorkDescription': 'work_description',
        'ContractorName': 'contractor_name',
        PROJECTCOST: 'project_cost',
        COST: 'project_cost',
        TOTALFEE: 'total_fee',
    },
    'Census': \
    {
        'Census Yr': 'census_year',
        'Res. ID': RESIDENT_ID,
        'Last Nm': LAST_NAME,
        'First Nm': FIRST_NAME,
        'Middle Nm': MIDDLE_NAME,
        'D.O.B. (mm/dd/yyyy format)': DATE_OF_BIRTH,
        'Occupation': OCCUPATION,
        'Mail to Code (Y if HOH; N if Not)': 'head_of_household',
        'Res. - St. #': RADDR_STREET_NUMBER,
        'Res. - St. # Suffix': RADDR_STREET_NUMBER_SUFFIX,
        'Res. - St. Name': RADDR_STREET_NAME,
        'Res. - Apt #': RADDR_APARTMENT_NUMBER,
        'Res. - Zip Code': RADDR_ZIP_CODE,
        'Party': PARTY_AFFILIATION,
        'Ward #': 'ward_number',
        'Precinct #': PRECINCT_NUMBER,
        'Voter Status': VOTER_STATUS,
    },
    'EjCommunities': \
    {
        'TOWN_NAME': TOWN_NAME,
        'COUNTY_NAME': 'county_name',
        'TRACT_NAME': 'tract_name',
        'Census Tract': 'census_tract',
        'Tot_Pop': TRACT_POPULATION,
        'AvgEnergyBurden': 'avg_energy_burden',
        'AvgEnergyBurden_Screen': 'avg_energy_burden_screen',
        'PCT_LowIncome': PCT_LOW_INCOME,
        'PCT_LowIncome_Screen': 'pct_low_income_screen',
        'FE_Comm': 'fe_comm',
        'FE_Comm.1': 'fe_comm_l',
        'EJ_Indicators': 'ej_indicators',
        'EJ_Comm_Screen': 'ej_comm_screen',
        'P_LDPNT_D2': 'p_ldpnt_d2',
        'P_DSLPM_D2': 'p_dslpm_d2',
        'P_CANCR_D2': 'p_cancr_d2',
        'P_RESP_D2': 'p_resp_d2',
        'P_PTRAF_D2': 'p_ptraf_d2',
        'P_PWDIS_D2': 'p_pwdis_d2',
        'P_PNPL_D2': 'p_pnpl_d2',
        'P_PRMP_D2': 'p_prmp_d2',
        'P_PTSDF_D2': 'p_ptsdf_d2',
        'P_OZONE_D2': 'p_ozone_d2',
        'P_PM25_D2': 'p_pm25_d2',
        'P_60_LDPNT_D2': 'p_60_ldpnt_d2',
        'P_60_DSLPM_D2': 'p_60_dslpm_d2',
        'P_60_CANCR_D2': 'p_60_cancr_d2',
        'P_60_RESP_D2': 'p_60_resp_d2',
        'P_60_PTRAF_D2': 'p_60_ptraf_d2',
        'P_60_PWDIS_D2': 'p_60_pwdis_d2',
        'P_60_PNPL_D2': 'p_60_pnpl_d2',
        'P_60_PRMP_D2': 'p_60_prmp_d2',
        'P_60_PTSDF_D2': 'p_60_ptsdf_d2',
        'P_60_OZONE_D2': 'p_60_ozone_d2',
        'P_60_PM25_D2': 'p_60_pm25_d2',
    },
    'Elections': \
    {
        '1st Ballot Mailed Disposition': BALLOT_MAILED_DISPOSITION.format( 1 ),
        '1st Ballot Received By': BALLOT_RECEIVED_BY.format( 1 ),
        '1st Ballot Rejection Reason': BALLOT_REJECTION_REASON.format( 1 ),
        '1st Ballot Sent By': BALLOT_SENT_BY.format( 1 ),
        '2nd Ballot Mailed Disposition': BALLOT_MAILED_DISPOSITION.format( 2 ),
        '2nd Ballot Received By': BALLOT_RECEIVED_BY.format( 2 ),
        '2nd Ballot Rejection Reason': BALLOT_REJECTION_REASON.format( 2 ),
        '2nd Ballot Sent By': BALLOT_SENT_BY.format( 2 ),
        '3rd Ballot Mailed Disposition': BALLOT_MAILED_DISPOSITION.format( 3 ),
        '3rd Ballot Received By': BALLOT_RECEIVED_BY.format( 3 ),
        '3rd Ballot Rejection Reason': BALLOT_REJECTION_REASON.format( 3 ),
        '3rd Ballot Sent By': BALLOT_SENT_BY.format( 3 ),
        'City/ Town Code': TOWN_CODE,
        'City/ Town Code Assigned Number': TOWN_CODE,
        'City/ Town Indicator': TOWN_INDICATOR,
        'City/ Town Name': TOWN_NAME,
        'City/Town Indicator': TOWN_INDICATOR,
        'CityTown Name': TOWN_NAME,
        'Date 1st Ballot Mailed': BALLOT_MAILED_DATE.format( 1 ),
        'Date 1st Ballot Received': BALLOT_RECEIVED_DATE.format( 1 ),
        'Date 2nd Ballot Mailed': BALLOT_MAILED_DATE.format( 2 ),
        'Date 2nd Ballot Received': BALLOT_RECEIVED_DATE.format( 2 ),
        'Date 3rd Ballot Mailed': BALLOT_MAILED_DATE.format( 3 ),
        'Date 3rd Ballot Received': BALLOT_RECEIVED_DATE.format( 3 ),
        'Election Type': ELECTION_TYPE,
        'First Name': FIRST_NAME,
        'Last Name': LAST_NAME,
        'Mailing Address - Apartment Number': MADDR_APARTMENT_NUMBER,
        'Mailing Address - City/Town': MADDR_CITY,
        'Mailing Address - State': MADDR_STATE,
        'Mailing Address - Street Number/Name': MADDR_STREET,
        'Mailing Address - Zip Code': MADDR_ZIP_CODE,
        'Mailing Address Line 1': MADDR_LINE.format( 1 ),
        'Mailing Address Line 2': MADDR_LINE.format( 2 ),
        'Mailing Address Line 3': MADDR_LINE.format( 3 ),
        'Mailing Address Line 4': MADDR_LINE.format( 4 ),
        'Mailing Address Line 5': MADDR_LINE.format( 5 ),
        'Middle Name': MIDDLE_NAME,
        'Party Affiliation': PARTY_AFFILIATION,
        'Party Enrolled': PARTY_AFFILIATION,
        'Record Seq. #': PARTY_AFFILIATION,
        'Residential Address - Apartment Number': RADDR_APARTMENT_NUMBER,
        'Residential Address - Street Name': RADDR_STREET_NAME,
        'Residential Address - Street Number': RADDR_STREET_NUMBER,
        'Residential Address - Street Suffix': RADDR_STREET_NUMBER_SUFFIX,
        'Residential Address - Zip Code': RADDR_ZIP_CODE,
        'Residential Address Apt Number': RADDR_APARTMENT_NUMBER,
        'Residential Address Street Name': RADDR_STREET_NAME,
        'Residential Address Street Number': RADDR_STREET_NUMBER,
        'Residential Address Street Suffix': RADDR_STREET_NUMBER_SUFFIX,
        'Residential Address Zip Code': RADDR_ZIP_CODE,
        'Title': TITLE,
        'Type of Election': ELECTION_TYPE,
        'Vot Delete Flag': 'vote_delete_flag',
        'Voter ID': RESIDENT_ID,
        'Voter ID Number': RESIDENT_ID,
        'Voter Status': VOTER_STATUS,
        'Voter Status r': VOTER_STATUS,
        'Voter Title': TITLE,
        'AV Flag': 'av_flag',
        'Date Application Received': 'date_application_received',
        'Election Date': ELECTION_DATE,
        'EV Type': 'ev_type',
        'Party Voted': PARTY_VOTED,
        'Poll ID Required': 'poll_id_required',
        'Precinct Number': PRECINCT_NUMBER,
        'Rec Sequence Number': 'rec_sequence_number',
        'Ward Number': 'ward_number',
    },
    'Employees': \
    {
        'TOWN/': DEPARTMENT,
        'LAST NAME': LAST_NAME,
        'FIRST NAME': FIRST_NAME,
        'POSITION': POSITION,
        '*REGULAR PAY': 'regular_pay',
        '*OFF-DUTY': 'off_duty_pay',
        '*OVERTIME': 'overtime_pay',
        '*OTHER': 'other_pay',
        '*RETRO': 'retroactive_pay',
        'TOTAL GROSS': 'total_gross_pay',
    },
    'Gender_2014': \
    {
        'Resident Id Number': RESIDENT_ID,
        'Gender': GENDER,
    },
    'PollingPlaces': \
    {
        'Year': YEAR,
        'Precinct': 'precinct',
        'Building': 'building',
        'Address': 'address',
    },
    'RawElectricEesRates': \
    {
        'Year': YEAR,
        'Electric Utility': ELECTRIC_UTILITY,
        'R-1': RESIDENTIAL_R1_RATE,
        'R-2': RESIDENTIAL_R2_RATE,
        'C&I': COMMERCIAL_RATE,
        'Notes': 'notes',
        'URL': 'url',
    },
    'RawElectricUsage': \
    {
        'year': YEAR,
        'Town': TOWN_NAME,
        'Sector': SECTOR,
        'Jan': JAN,
        'Feb': FEB,
        'Mar': MAR,
        'Apr': APR,
        'May': MAY,
        'Jun': JUN,
        'Jul': JUL,
        'Aug': AUG,
        'Sept': SEP,
        'Oct': OCT,
        'Nov': NOV,
        'Dec': DEC,
        'Annual': ANNUAL_ELECTRIC_USAGE,
    },
    'RawGasEesRates': \
    {
        'Year': YEAR,
        'Gas Utility': GAS_UTILITY,
        'Residential': RESIDENTIAL_RATE,
        'C&I': COMMERCIAL_RATE,
        'Notes': 'notes',
        'URL': 'url',
        'Docket Number': 'docket_number',
    },
    'RawGasUsage': \
    {
        'year': YEAR,
        'Town': TOWN_NAME,
        'Sector': SECTOR,
        'Jan': JAN,
        'Feb': FEB,
        'Mar': MAR,
        'Apr': APR,
        'May': MAY,
        'Jun': JUN,
        'Jul': JUL,
        'Aug': AUG,
        'Sept': SEP,
        'Oct': OCT,
        'Nov': NOV,
        'Dec': DEC,
        'Annual': ANNUAL_GAS_USAGE,
    },
    'RawGeographicReport': \
    {
        'year': YEAR,
        'County': COUNTY,
        'Town': TOWN_NAME,
        'Zip Code': ZIP,
        'Sector': SECTOR,
        'Annual Electric Usage (MWh)': ANNUAL_ELECTRIC_USAGE,
        'Annual Electric Savings (MWh)': ANNUAL_ELECTRIC_SAVINGS,
        'Electric Incentives': ELECTRIC_INCENTIVES,
        'Annual Gas Usage (Therms)': ANNUAL_GAS_USAGE,
        'Annual Gas Savings (Therms)': ANNUAL_GAS_SAVINGS,
        'Gas Incentives': GAS_INCENTIVES,
    },
    'RawLocalElectionResults': \
    {
        'P-1': PRECINCT_1,
        'P-2': PRECINCT_2,
        'P-3': PRECINCT_3,
        'P-4': PRECINCT_4,
        'P-5': PRECINCT_5,
        'P-6': PRECINCT_6,
        'P-7/7A': PRECINCT_7,
        'P-8': PRECINCT_8,
        'P-9': PRECINCT_9,
        'election_date': ELECTION_DATE,
        'PRECINCTS:': OFFICE_OR_CANDIDATE,
    },
    'Solar': \
    {
        'FIRST_NAME': FIRST_NAME,
        'MIDDLE_NAME': MIDDLE_NAME,
        'LAST_NAME': LAST_NAME,
        'SITE_ADDR': SITE_ADDRESS,
        'TOWN': 'town',
        'F1_EST_KW': 'estimated_kw',
        'CREDIT': 'credit',
        'ELEC_UTIL': 'electric_utility',
        'EFFCTV_RATE': 'effective_rate',
        'EST_KWH_CN': 'estimated_kwh',
        'MAX_BEHAV_SCORE': 'max_behavior_score',
        'PHONE': PHONE,
    },
    'Water': \
    {
        'serv_id': SERVICE_ID,
        'acct_no': ACCOUNT_NUMBER,
        'service': SERVICE,
        'last': LAST_NAME,
        'first': FIRST_NAME,
        'number': ADDR_STREET_NUMBER,
        'name': ADDR_STREET_NAME,
        'meter_no': METER_NUMBER,
        'desc': DESCRIPTION,
        'tran_date': TRANSACTION_DATE,
        'type_str': TRANSACTION_TYPE,
        'units': UNITS,
        'prior_date': PRIOR_DATE,
        'cur_date': CURRENT_DATE,
        'cur_read': CURRENT_READING,
        'prior_read': PRIOR_READING,
        'service_type': SERVICE_TYPE,
    },
}


COLUMN_GROUP = \
{
    'RESIDENT':
    [
        RESIDENT_ID,
        AGE,
        GENDER,
        LAST_NAME,
        FIRST_NAME,
        OCCUPATION,
        VOTED,
        PARTY_AFFILIATION,
    ],
    'VOTER':
    [
        VOTER_ENGAGEMENT_SCORE,
        PRIMARY_ELECTIONS_VOTED,
        LOCAL_ELECTIONS_VOTED,
        STATE_ELECTIONS_VOTED,
        SPECIAL_ELECTIONS_VOTED,
        TOWN_MEETINGS_ATTENDED,
        EARLY_VOTES,
        PRECINCT_NUMBER,
    ],
    'SCORE':
    [
        PARTY_PREFERENCE_SCORE,
        LEGACY_PREFERENCE_SCORE,
        LIKELY_DEM_SCORE,
        LEGACY_DEM_SCORE,
    ],
    'ADDRESS':
    [
        NORMALIZED_STREET_NUMBER,
        RADDR_STREET_NUMBER_SUFFIX,
        LADDR_ALT_STREET_NUMBER,
        NORMALIZED_STREET_NAME,
        RADDR_APARTMENT_NUMBER,
    ],
    'HOME':
    [
        TOTAL_ASSESSED_VALUE,
        IS_HOMEOWNER,
        IS_FAMILY,
    ],

}
COLUMN_ORDER = \
{
    'Assessment':
    [
        PARCEL_ID,
    ],
    'Census':
    [
        RESIDENT_ID,
    ],
    'ElectionHistory':
    [
    ],
    'ElectionModel_01':
    [
        RESIDENT_ID,
    ],
    'ElectionModel_02':
    [
        RESIDENT_ID,
    ],
    'ElectionModel_03':
    [
        RESIDENT_ID,
    ],
    'Employees':
    [
    ],
    'Gender_2014':
    [
    ],
    'LocalElectionResults':
    [
        ELECTION_DATE,
        OFFICE,
        CANDIDATE,
    ],
    'Partisans':
    [
        * COLUMN_GROUP['RESIDENT'],
        * COLUMN_GROUP['VOTER'],
        * COLUMN_GROUP['ADDRESS'],
        ZONING_CODE_1,
        PARTISAN_SCORE,
        RECENT_LOCAL_ELECTIONS_VOTED,
    ],
    'PollingPlaces':
    [
    ],
    'Precincts':
    [
    ],
    'RawElectricUsage':
    [
        YEAR,
        TOWN_NAME,
        SECTOR,
        JAN,
        FEB,
        MAR,
        APR,
        MAY,
        JUN,
        JUL,
        AUG,
        SEP,
        OCT,
        NOV,
        DEC,
        ANNUAL_ELECTRIC_USAGE,
    ],
    'RawGasUsage':
    [
        YEAR,
        TOWN_NAME,
        SECTOR,
        JAN,
        FEB,
        MAR,
        APR,
        MAY,
        JUN,
        JUL,
        AUG,
        SEP,
        OCT,
        NOV,
        DEC,
        ANNUAL_GAS_USAGE,
    ],
    'RawGeographicReport':
    [
        YEAR,
        COUNTY,
        TOWN_NAME,
        ZIP,
        SECTOR,
        ANNUAL_ELECTRIC_USAGE,
        ANNUAL_ELECTRIC_SAVINGS,
        ELECTRIC_INCENTIVES,
        ANNUAL_GAS_USAGE,
        ANNUAL_GAS_SAVINGS,
        GAS_INCENTIVES,
    ],
    'RawLocalElectionResults':
    [
        ELECTION_DATE,
        OFFICE_OR_CANDIDATE,
    ],
    'Residents':
    [
        * COLUMN_GROUP['RESIDENT'],
        * COLUMN_GROUP['SCORE'],
        * COLUMN_GROUP['VOTER'],
        * COLUMN_GROUP['HOME'],
        * COLUMN_GROUP['ADDRESS'],
        PARCEL_ID,
        ZONING_CODE_1,
    ],
    'Solar':
    [
    ],
    'Streets':
    [
    ],
    'Water':
    [
        SERVICE_ID,
        LAST_NAME,
        FIRST_NAME,
        ADDR_STREET_NUMBER,
        ADDR_STREET_NAME,
        SERVICE_TYPE,
        DESCRIPTION,
        SERVICE,
        CURRENT_DATE,
        PRIOR_DATE,
        CURRENT_READING,
        PRIOR_READING,
    ],
}

COLUMN_ORDER['Partisans_' + D] = COLUMN_ORDER['Partisans']
COLUMN_ORDER['Partisans_' + R] = COLUMN_ORDER['Partisans']

# Information on how to publish databases
PUBLISH_INFO = \
{
    'student': \
    {
        'number_columns': True,
        'drop_table_names':
        [
            'Employees',
            'Partisans_' + D,
            'Partisans_' + R,
        ],
        'encipher_column_names':
        [
            RESIDENT_ID
        ],
        'drop_column_names':
        [
            FIRST_NAME,
            MIDDLE_NAME,
            LAST_NAME,
            DATE_OF_BIRTH,
            OWNER_1_NAME,
            OWNER_2_NAME,
            OWNER_3_NAME,
            PHONE,
            GRANTOR,
            PREVIOUS_GRANTOR,
            PARTY_AFFILIATION,
            PARTY_PREFERENCE_SCORE,
            LEGACY_PREFERENCE_SCORE,
            GENDER,
            IS_FAMILY,
            LIKELY_DEM_SCORE,
            LEGACY_DEM_SCORE,
            D,
            R,
            ABSENT,
            VOTED_BOTH,
            CHANGED_AFFILIATION,
            PARTY_VOTED_HISTORY,
            LIKELY_DEM_COUNT,
            LIKELY_REPUB_COUNT,
            LOCAL_DEM_VOTER_COUNT,
            LOCAL_REPUB_VOTER_COUNT,
            MEAN_LIKELY_DEM_SCORE,
            MEAN_PARTY_PREFERENCE_SCORE,
            MEAN_LIKELY_DEM_VOTER_ENGAGEMENT_SCORE,
            MEAN_LIKELY_REPUB_VOTER_ENGAGEMENT_SCORE,
        ]
    },
    'town': \
    {
        'number_columns': True,
        'drop_table_names':
        [
            'Employees',
            'Gender_2014',
            'Lookup',
            'Partisans_' + D,
            'Partisans_' + R,
        ],
        'encipher_column_names':
        [
        ],
        'drop_column_names':
        [
            PARTY_AFFILIATION,
            PARTY_PREFERENCE_SCORE,
            LEGACY_PREFERENCE_SCORE,
            GENDER,
            IS_FAMILY,
            LIKELY_DEM_SCORE,
            LEGACY_DEM_SCORE,
            D,
            R,
            ABSENT,
            VOTED_BOTH,
            CHANGED_AFFILIATION,
            PARTY_VOTED_HISTORY,
            LIKELY_DEM_COUNT,
            LIKELY_REPUB_COUNT,
            LOCAL_DEM_VOTER_COUNT,
            LOCAL_REPUB_VOTER_COUNT,
            MEAN_LIKELY_DEM_SCORE,
            MEAN_PARTY_PREFERENCE_SCORE,
            MEAN_LIKELY_DEM_VOTER_ENGAGEMENT_SCORE,
            MEAN_LIKELY_REPUB_VOTER_ENGAGEMENT_SCORE,
        ]
    },
    'research': \
    {
        'number_columns': True,
        'drop_table_names':
        [
        ],
        'encipher_column_names':
        [
        ],
        'drop_column_names':
        [
        ]
    },
}



# Convert likely_dem_score to party_preference_score
def likely_dem_to_party_preference_score( dem_score ):

    if pd.isnull( dem_score ):
        pref_score = ''

    else:

        # Determine party prefix
        pref_score = ( 'D' if ( dem_score > 0 ) else ( 'R' if ( dem_score < 0 ) else '-' ) )

        # Append numeric score
        pref_score += ' ' + str( int( abs( dem_score ) ) ).zfill( 3 )

    return pref_score


# Read series of input files in specified directory
def read_excel_files( input_directory, column_labels, skiprows ):

    # Initialize empty dataframe
    df_xl = pd.DataFrame()

    # Construct dataframe from input files found in directory
    for filename in os.listdir( input_directory ):

        input_path = input_directory + '/' + filename
        print( 'Reading "{0}"'.format( input_path ) )
        df = pd.read_excel( input_path, dtype=object, skiprows=skiprows )

        # Optionally add extra columns and populate with filename fragments delimited by underscore - e.g., given input file moo_1234.xlsx, column value will be 'moo'
        if column_labels:
            labels = column_labels.split( ',' )
            values = filename.split( '_' )

            for label in labels:
                value = values.pop(0)
                df[label] = value
                print( '- Added column "{0}" with value "{1}"'.format( label, value ) )

        # Append partial input to dataframe
        df_xl = df_xl.append( df, sort=True )

    return df_xl


# Rename columns according to mappings
def rename_columns( df, table_name ):

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()

    # If any column names are not mapped, report and abort
    not_in = []
    if table_name in CONSISTENT_COLUMN_NAMES:
        for col_name in df.columns:
            if col_name not in CONSISTENT_COLUMN_NAMES[table_name]:
                not_in.append( col_name )
    else:
        not_in = list( df.columns )

    if len( not_in ):
        print( '!!! CONSISTENT_COLUMN_NAMES mappings missing for table {0}, {1} columns: {2}'.format( table_name, len(not_in), not_in ) )
        exit()

    # Correct misspelling and mislabeling
    if table_name in CONSISTENT_COLUMN_NAMES:

        bf = df.columns
        df = df.rename( index=str, columns=CONSISTENT_COLUMN_NAMES[table_name] )
        af = df.columns
        if len( bf.difference( af ) ):
            print( 'Renamed columns -' )
            print( '  From: {0}'.format( list( bf.difference( af ) ) ) )
            print( '    To: {0}'.format( list( af.difference( bf ) ) ) )

    return df


# Open the SQLite database
def open_database( filename, b_create):

    print( '\nOpening database: ' + filename )

    # Optionally delete pre-existing database
    if b_create:
        if os.path.exists( filename ):
            print( ' Deleting...' )
            os.remove( filename )
        print( ' Creating...' )

    print( ' Connecting...' )
    conn = sqlite3.connect( filename )
    cur = conn.cursor()

    engine = sqlalchemy.create_engine( 'sqlite:///' + filename )

    return conn, cur, engine


def read_database( input_filename ):

    # Open the input database
    conn, cur, engine = open_database( input_filename, False )

    # Fetch names of all tables
    cur.execute( 'SELECT name FROM sqlite_master WHERE type="table" AND name NOT LIKE "sqlite_%";' )
    rows = cur.fetchall()

    # Built pandas representation of input database
    print( '' )
    input_db = {}
    for row in rows:
        table_name = row[0]
        print( 'Reading table', table_name )
        input_db[table_name] = pd.read_sql_table( table_name, engine, parse_dates=True )

    return input_db


def copy_views( input_filename, views_filename, output_filename ):

    # Create a copy of the input file
    copyfile( input_filename, output_filename )

    # Open databases
    vw_conn, vw_cur, vw_engine = open_database( views_filename, False )
    out_conn, out_cur, out_engine = open_database( output_filename, False )

    # Fetch views
    vw_cur.execute( 'SELECT sql FROM sqlite_master WHERE type="view"' )
    rows = vw_cur.fetchall()

    # Recreate views in output file
    print( '' )
    input_db = {}
    for row in rows:
        print( row[0] )
        out_cur.execute( row[0] )

    out_conn.commit


# Create table with specified name and model
def create_table( table_name, conn, cur, columns=None, df=None ):

    if ( columns is None ) and ( df is None ):
        print( "!!! create_table() missing required parameter: either 'columns' or 'df'" )
        exit()

    else:

        # Initialize and reorder columns
        if columns is None:
            columns = df.columns
        columns = reorder_columns( table_name, list( columns ) )

        # Drop table if it already exists
        cur.execute( 'DROP TABLE IF EXISTS ' + table_name )

        # Generate SQL command
        create_sql = 'CREATE TABLE ' + table_name + ' ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE'
        for col_name in columns:
            sqltype = pdtype_to_sqltype( df, col_name )
            create_sql += ', "{0}" {1}'.format( col_name, sqltype )
        create_sql += ' )'

        # Execute the SQL command
        print( '' )
        print( create_sql )
        cur.execute( create_sql )

        # Optionally output dataframe to database and test spreadsheets
        if df is not None:
            df.to_sql( table_name, conn, if_exists='append', index=False )
            df = df.reindex( columns=columns )
            df.index = 1 + df.reset_index().index
            df.to_csv( '../test/' + table_name + '.csv', index_label=ID )

        conn.commit()


def reorder_columns( table_name, columns ):

    # Initialize list of remaining columns
    remaining = columns.copy()

    # Get column names listed in preferred order
    preferred_order = COLUMN_ORDER[table_name] if table_name in COLUMN_ORDER else []

    # Initialize empty result
    result = []

    # Iterate over column names listed in preferred order
    for col_name in preferred_order:

        if col_name in remaining:

            # Add to result
            result.append( col_name )

            # Remove from list of remaining remaining
            remaining.remove( col_name )

        else:
            print( '!!! COLUMN_ORDER for "{0}" table lists unexpected column name: "{1}"'.format( table_name, col_name ) )
            exit()

    # Append remaining remaining to result
    result += remaining

    # Correct misspelling and mislabeling
    if columns != result:
        print( '' )
        print( 'Reordered columns -' )
        print( '  From: {0}'.format( columns ) )
        print( '    To: {0}'.format( result ) )

    return result


def pdtype_to_sqltype( df, col_name ):

    if df is None:
        sqltype = 'TEXT'
    else:
        pdtype = str( df[col_name].dtype )

        if pdtype.startswith( 'float' ):
            sqltype = 'FLOAT'
        elif pdtype.startswith( 'int' ):
            sqltype = 'INT'
        else:
            sqltype = 'TEXT'

    return sqltype


# Prepare dataframe for saving to database
def prepare_for_database( df, table_name ):

    # Correct misspelled and inconsistent column names
    df = rename_columns( df, table_name )

    # Fix zip codes
    df = fix_zip_codes( df )

    # Fix integer columns
    df = fix_int_columns( df )

    # Fix date columns
    df = fix_date_columns( df )

    # Strip apostrophes
    df = strip_apostrophes( df )

    # Replace carriage returns
    df = replace_cr( df )

    # Collapse spaces
    df = collapse_spaces( df )

    # Strip leading and trailing spaces
    df = strip_ends( df )

    return df


# Remove apostrophes and apostrophe-like characters from name and address columns
def strip_apostrophes( df ):

    # Strip apostrophes from columns that follow strict conditions
    for column_name in df.columns:

        # Is this column a name or address fragment, but not a number?
        b_name = ( column_name.lower().find( 'name' ) != -1 )
        b_addr = ( column_name.lower().find( 'address' ) != -1 )
        b_num = ( column_name.lower().find( 'number' ) != -1 )

        if ( b_name or b_addr ) and not b_num:

            # Is the column type string?
            b_str = df[column_name].dtype == object

            if b_str:

                # Does this column have any apostrophes?
                sr1 = df[column_name].str.contains( "'" )
                sr2 = df[column_name].str.contains( "`" )
                counts1 = sr1.value_counts()
                counts2 = sr2.value_counts()
                b_has_apostrophe = ( True in counts1 ) or ( True in counts2 )

                # If the column has apostrophes...
                if b_has_apostrophe:

                    # Strip apostrophes from the column
                    df[column_name] = df[column_name].str.replace( r"['`]", '' )

    return df


# Replace all occurrences of carriage return character with single space
def replace_cr( df ):

    df = df.replace( to_replace=r'\r', value=' ', regex=True )

    return df


# Replace all occurrences of multiple consecutive spaces with single space
def collapse_spaces( df ):

    df = df.replace( to_replace=r' +', value=' ', regex=True )

    return df


# Strip leading and trailing spaces
def strip_ends( df ):

    df = df.replace( to_replace=r'^\s', value='', regex=True )
    df = df.replace( to_replace=r'\s$', value='', regex=True )

    return df


# Fix all zip codes in specified dataframe
def fix_zip_codes( df ):

    for column_name in df.columns:

        if column_name.lower().find( 'zip' ) != -1:
            df[column_name] = fix_zip_code( df[column_name] )

    return df


# Fix integer columns in specified dataframe
def fix_int_columns( df ):

    for column_name in df.columns:

        if ( column_name.lower().find( 'year' ) != -1 ) or ( column_name.lower().find( 'number' ) != -1 ) or ( column_name.lower().find( 'reading' ) != -1 ) or ( column_name.lower().find( 'phone' ) != -1 ):
            df[column_name] = df[column_name].fillna( '' )

    return df


# Reformat zip code when expressed as numeric value
def fix_zip_code( column ):

    # Build dictionary of replacement values
    dc_repl = {}
    for val in column.value_counts().index:

        if type( val ) in ( int, float ):
            # Convert numeric value to formatted zip code
            zip_plus_four = make_zip_code( int( val ) )

        else:
            # Retain original value
            zip_plus_four = val

        # Add formatted value to replacement dictionary
        dc_repl[val] = zip_plus_four

    # Replace original values with formatted zip codes
    column = column.replace( to_replace=dc_repl )

    return column


# Convert integer value to formatted zip code
def make_zip_code( i_val ):

    if i_val > 99999:
        # Value represents 9-digit zip
        i_four = i_val % 10000
        s_five = '{:05d}'.format( ( i_val - i_four ) // 10000 )
        s_four = '-{:04d}'.format( i_four )
    else:
        # Value represents 5-digit zip
        s_five = '{:05d}'.format( i_val )
        s_four = ''

    return '{0}{1}'.format( s_five, s_four )


# Force pandas to parse unparsed dates
def fix_date_columns( df ):

    # Determine which columns should represent dates
    expected_date_cols = get_named_date_columns( df )

    # Determine which columns represent dates
    date_cols = get_date_columns( df )

    fixed_cols = []

    # Fix columns that should be dates, but are not
    for col_name in expected_date_cols:

        if col_name not in date_cols:
            fixed_cols.append( col_name )
            df[col_name] = pd.to_datetime( df[col_name], infer_datetime_format=True, errors='coerce' )

    if len( fixed_cols ):
        print( '\nFixed date columns:', fixed_cols )

    return df


NOT_DATES = \
[
    OFFICE_OR_CANDIDATE,
]

# Find all columns whose names imply that they should represent dates
def get_named_date_columns( df ):
    date_cols = []
    for col_name in df.columns:
        if ( col_name.lower().find( 'date' ) != -1 ) and ( col_name not in NOT_DATES ):
            date_cols.append( col_name )
    return date_cols


# Find all columns that represent dates
def get_date_columns( df ):
    date_cols = []
    for col_name in df.columns:
        if df[col_name].dtype == 'datetime64[ns]':
            date_cols.append( col_name )
    return date_cols


# Encypher sensitive data
def encipher_column( df, column_name ):

    if column_name in df:
        df[column_name] = df[column_name].apply( encipher )

    return df


# Encipher alphanumeric characters in string
def encipher( s ):
    return transform( s, 1 ) if s else ''

# Decipher encipher() result
def decipher( s ):
    return transform( s, -1 ) if s else ''

TRANSFORM_SEQUENCE = string.digits + string.ascii_uppercase
TRANSFORM_SEQUENCE_LENGTH = len( TRANSFORM_SEQUENCE )
TRANSFORM_SHIFT = 13

def transform( text, direction ):

    trans = ''

    for c in text:

        offset = TRANSFORM_SEQUENCE.find( c )

        if offset == -1:
            trans += c
        else:
            trans += TRANSFORM_SEQUENCE[ ( offset + ( TRANSFORM_SHIFT * direction ) ) % TRANSFORM_SEQUENCE_LENGTH ]

    return trans


def publish_database( input_db, output_filename, publish_info ):

    drop_table_names = publish_info['drop_table_names']
    drop_column_names = publish_info['drop_column_names']
    encipher_column_names = publish_info['encipher_column_names']
    number_columns = publish_info['number_columns']

    print( '' )
    print( 'Publishing database', output_filename )
    print( '- Dropping tables {0}'.format( drop_table_names ) )
    print( '- Dropping columns {0}'.format( drop_column_names ) )
    print( '- Enciphering columns {0}'.format( encipher_column_names ) )
    print( '' )

    output_db = {}
    for table_name in input_db:
        if table_name not in drop_table_names:
            print( 'Sanitizing table {0}'.format( table_name ) )
            output_db[table_name] = input_db[table_name].drop( columns=drop_column_names, errors='ignore' )
            for col_name in encipher_column_names:
                output_db[table_name] = encipher_column( output_db[table_name], col_name )

            if number_columns:
                n_cols = len( output_db[table_name].columns )
                num_width = len( str( n_cols ) )
                col_idx = 0
                for column_name in output_db[table_name].columns:
                    col_idx += 1
                    output_db[table_name] = output_db[table_name].rename( columns={ column_name: str( col_idx ).zfill( num_width ) + '-' + column_name } )


    # Open output database
    conn, cur, engine = open_database( output_filename, True )

    # Save result to database
    print( '' )
    for table_name in output_db:
        print( 'Publishing table', table_name )
        df = output_db[table_name]
        df.to_sql( table_name, conn, index=False )



def report_elapsed_time( prefix='\n', start_time=START_TIME ):
    elapsed_time = round( ( time.time() - start_time ) * 1000 ) / 1000
    minutes, seconds = divmod( elapsed_time, 60 )
    ms = round( ( elapsed_time - int( elapsed_time ) ) * 1000 )
    print( prefix + 'Elapsed time: {:02d}:{:02d}.{:d}'.format( int( minutes ), int( seconds ), ms ) )
