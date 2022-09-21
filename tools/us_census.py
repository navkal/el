# Copyright 2022 Energize Lawrence.  All rights reserved.

import argparse
import census
import numpy as np
import pandas as pd
import us


ESSEX_COUNTY_CODE = '009'


def _get_census_client():
    return census.Census(args.api_key)


def get_essex_income_table():
    census_client = _get_census_client()

    # More codes available at https://api.census.gov/data/2020/acs/acs5/variables.html
    median_income_code = 'B19013_001E'
    population_code = 'B01003_001E'

    # See https://pypi.org/project/census/ for example usage,
    # https://github.com/datamade/census for source code
    response = census_client.acs5.state_county_blockgroup(
        fields=('NAME', median_income_code, population_code),
        state_fips=us.states.MA.fips,
        county_fips=ESSEX_COUNTY_CODE,
        blockgroup=census.Census.ALL,
    )

    df = (
        pd.DataFrame.from_dict(response)
        .rename({median_income_code: 'income', population_code: 'population'}, axis=1)
    )
    # Some income values come back as -666666666, maybe to indicate they're missing?
    # We'll represent them as nan.
    df.loc[df['income'] == -666666666.0, 'income'] = np.nan
    return df


def get_essex_heating_fuel_table():
    census_client = _get_census_client()

    # See https://api.census.gov/data/2020/acs/acs5/groups/B25040.html for descriptions
    # of variables in this group.
    heating_fuel_group = 'B25040'

    response = census_client.acs5.state_county_blockgroup(
        fields=('NAME', f'group({heating_fuel_group})'),
        state_fips=us.states.MA.fips,
        county_fips=ESSEX_COUNTY_CODE,
        blockgroup=census.Census.ALL,
    )

    df = pd.DataFrame.from_dict(response)

    return df


if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Retrieve US Census data' )
    parser.add_argument( '-k', dest='api_key',  help='Credentials required by census API', required=True )
    args = parser.parse_args()

    print('Population and income table:')
    df = get_essex_income_table()
    print( df )
    df.to_excel( './out/us_census_income.xlsx', index=False )

    print()

    print('Heating fuel table:')
    df = get_essex_heating_fuel_table()
    print( df )
    df.to_excel( './out/us_census_fuel.xlsx', index=False )
