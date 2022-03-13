import sqlite3

import numpy as np
import pandas as pd
import statsmodels.api as sm
import sklearn.linear_model
import scipy.stats

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('display.width', 1000)


db_path = '../db/mass_save.sqlite'
list_tables_query = """
SELECT name FROM sqlite_master
WHERE type='table';
"""
select_all_query = """
SELECT * FROM %s;
"""
data = {}
with sqlite3.connect(db_path) as con:
    cursor = con.cursor()
    cursor.execute(list_tables_query)
    table_names = [name for name, in cursor.fetchall()]
    for table_name in table_names:
        df = pd.read_sql_query(select_all_query % table_name, con)
        if 'year' in df.columns:
            df = df.astype({'year': int})
        data[table_name] = df


analysis_df = data['Analysis'].copy()
analysis_df_with_towns = pd.merge(data['Towns'].drop('id', axis=1), analysis_df, on=('town_name'))
numeric_columns = [
    'annual_electric_usage_mwh',
    'annual_electric_savings_mwh',
    'electric_incentives_$',
    'annual_gas_usage_therms',
    'annual_gas_savings_therms',
    'gas_incentives_$',
    'electric_ees_$',
    'gas_ees_$',
    'electric_ees_minus_incentives_$',
    'gas_ees_minus_incentives_$',
    'incentives_per_saved_mwh_$',
    'incentives_per_saved_therm_$',
    'combined_ees_in_$',
    'combined_incentives_out_$',
    'combined_ees_minus_incentives_$',
]

for column_name in numeric_columns:
    analysis_df_with_towns['per_person_' + column_name] = analysis_df_with_towns[column_name] / analysis_df_with_towns['population']

numeric_columns += ['per_person_' + column_name for column_name in numeric_columns]

averages = (
    analysis_df_with_towns
    .drop('id', axis=1)
    .groupby(['town_name', 'sector'])
    [numeric_columns]
    .mean()
)
averages.columns = ['avg_' + col for col in averages.columns]


def _compute_increase_rate_for_one_time_series(df, column):
    duplicates = df['year'].duplicated()
    assert not duplicates.any(), duplicates
    exog = df[['year']]
    target = df[column]
    exog_w_const = sm.add_constant(exog, prepend=False)
    mod = sm.OLS(target, exog=exog_w_const)
    res = mod.fit()
    return res.params['year']


def _compute_increase_rates(df, target):
    rates_of_increase = (
        df
        .groupby(['sector', 'town_name'])
        .apply(lambda group: _compute_increase_rate_for_one_time_series(group, target))
    )
    rates_of_increase.name = 'rate_of_increase_' + target
    return rates_of_increase


target = 'per_person_combined_incentives_out_$'
rates_of_increase = _compute_increase_rates(analysis_df_with_towns, target)

# double check using one town and scikit-learn instead of statsmodels
abington_total = analysis_df_with_towns[
    (analysis_df_with_towns['town_name'] == 'Abington')
    & (analysis_df_with_towns['sector'] == 'Total')
]
model = sklearn.linear_model.LinearRegression()
model.fit(abington_total[['year']].values, abington_total[target].values)
np.testing.assert_almost_equal(
    model.coef_[0],
    rates_of_increase['Total', 'Abington'],
)

df = pd.merge(averages, rates_of_increase, on=('sector', 'town_name'))

df = pd.merge(data['Towns'].drop('id', axis=1), df.reset_index(level='sector'), on=('town_name'))


# just look at total usage rather than individual sectors, to simplify things
total = df[df['sector'] == 'Total']

# Rank towns by average EES - incentives.  This measures how much each town loses annually via the program.
total[['town_name', 'avg_combined_ees_minus_incentives_$']].set_index('town_name').sort_values('avg_combined_ees_minus_incentives_$')

# Rank towns by average per-person EES - incentives.  This measures how much a resident of a town loses annually via the program.
total[['town_name', 'avg_per_person_combined_ees_minus_incentives_$']].set_index('town_name').sort_values('avg_per_person_combined_ees_minus_incentives_$')

# Rank towns by the rate at which their per-person incentive influx is increasing.
# This measures how well a town is doing at improving its ability to get incentives.
total[['town_name', 'rate_of_increase_per_person_combined_incentives_out_$']].set_index('town_name').sort_values('rate_of_increase_per_person_combined_incentives_out_$')

# Try predicting avg_per_person_combined_incentives_out_$ as a function of pct_energy_burdened,
# to see if energy-burdened towns' residents are disproportionately benefiting from or losing
# out on incentives.  (It turns out that there isn't a strong relationship.)
exog = total[['pct_energy_burdened']]
target = 'avg_per_person_combined_incentives_out_$'
exog_w_const = sm.add_constant(exog, prepend=False)
mod = sm.OLS(total[target], exog=exog_w_const)
res = mod.fit()
print(res.summary())

