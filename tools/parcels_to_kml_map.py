# Copyright 2025 Energize Andover.  All rights reserved.

######################
#
# Data structures used by parcels_to_csv.py
#
######################

# Name of parcels table
TABLE = 'Assessment_L_Parcels'

# Columns to be extracted from parcels table
COLUMNS = \
[
    'ward_number',
    'heating_fuel_description',
    'vision_link',
    'latitude',
    'longitude',
]

# Map from column values to visual attributes
MAP = \
{
    'ward_number':
    {
        'A': 'red',
        'B': 'orange',
        'C': 'yellow',
        'D': 'blue',
        'E': 'purple',
        'F': 'pink',
    },
    'heating_fuel_description':
    {
        'Electric': 'triangle',
        'Oil': 'circle',
        'Gas': 'square',
    },
}
