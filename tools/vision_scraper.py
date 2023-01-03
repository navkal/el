# Copyright 2022 Energize Andover.  All rights reserved.

from bs4 import BeautifulSoup

import requests
import warnings
warnings.filterwarnings( 'ignore' )

LOWEST_GOOD_PID = 266

ls_pids = []

for i in range( LOWEST_GOOD_PID, LOWEST_GOOD_PID+10 ):
    url = 'https://gis.vgsi.com/lawrencema/parcel.aspx?pid=' + str( i )
    rsp = requests.get( url, verify=False )

    if ( rsp.url == url ):
        ls_pids.append( i )

        soup = BeautifulSoup(rsp.text, 'html.parser')

        b_fuel = False
        fuel = ''

        table = soup.find('table', id="MainContent_ctl01_grdCns")
        # print( len(table ) )
        for r in table:
            if not r.string:
                for d in r:
                    if b_fuel:
                        fuel = d.string
                    b_fuel = ( d.string == 'Heating Fuel' )

        print( i, 'Heating Fuel is', fuel )


    print( '{}: Found {} good PIDs'.format( i, len( ls_pids ) ) )
    print( ls_pids )

print( ls_pids )


#https://www.w3schools.com/python/ref_requests_response.asp
#https://gis.vgsi.com/lawrencema/parcel.aspx?pid=266