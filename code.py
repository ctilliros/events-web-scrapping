from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import re 
import json 


import sys
sys.path.insert(1, '../')
from inicosia_config import *

import psycopg2
conn = psycopg2.connect(host=host, database=database_events, user=user, password=password_events)
cursor = conn.cursor()

# Create database if not exists
sql ="SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s ";
cursor.execute(sql,(database_events,))
exists = cursor.fetchone()
if not exists:
	sql = 'CREATE DATABASE %s;'
	cursor.execute(sql,(database_events))
else:
	print("Database exists")

# Create table for facebook events if does not exists
sql = 'CREATE TABLE IF NOT EXISTS fb_events (id SERIAL NOT NULL, event_id BIGINT NOT NULL, \
    event_name text COLLATE pg_catalog."default",  \
    event_type text COLLATE pg_catalog."default",  \
    startdate text,  \
    enddate text ,  \
    streetAddress text,  \
    postalCode text, \
    event_location text ,\
    event_link text ,\
    latitude double precision ,\
    longitude double precision ,\
    UNIQUE(event_id),\
    CONSTRAINT fb_events_pkey PRIMARY KEY (id));'
cursor.execute(sql, )
conn.commit()


# Create table for eventbrite events if does not exists
sql = 'CREATE TABLE IF NOT EXISTS eventbrite_events (id SERIAL NOT NULL, event_id BIGINT NOT NULL, \
    event_name text COLLATE pg_catalog."default",  \
    event_type text COLLATE pg_catalog."default",  \
    startdate text,  \
    enddate text ,  \
    streetAddress text,  \
    postalCode text, \
    event_location text ,\
    event_link text ,\
    latitude double precision ,\
    longitude double precision ,\
    UNIQUE(event_id),\
    CONSTRAINT eventbrite_events_pkey PRIMARY KEY (id));'
cursor.execute(sql, )
conn.commit()   


### Next three lines do not open firefox app
# !brew install geckodriver
import time
from timeloop import Timeloop
from datetime import timedelta

tl = Timeloop()
@tl.job(interval=timedelta(seconds=150))
def run():
    options = Options()
    # options.add_argument('--headless')
    options.headless = True
	### Load data from facebook page ###
    import requests
    url = "https://mobile.facebook.com/pg/NicosiaMunicipality/events/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    get_details = soup.find_all("span", attrs={"class":"bv"})

    date_list = []
    name_list = []

    get_link = soup.find_all("span", attrs={"class":"bz"})
    event_link = []

    for links in get_link:
        for link in links.find_all('a', href=True):
            if link.text:
                matches = re.findall(r'/events/(.+?)?acontext',link['href'])
                event_link.append(matches[0][:-1])


    event_date_list = []
    event_location_list = []
    names = []
    types = []
    latitude = []
    longitude = []
    for k in event_link:
        sql = 'select * from fb_events where event_id = %s;'
    #     cursor.execute("rollback")
        cursor.execute(sql,(int(k),))
        row = cursor.fetchone()
        conn.commit()
        if row:
            print("To event einai idi kataxorimeno")
        else:
            url = "https://mobile.facebook.com/events/"+k+"/"
            page = requests.get(url)
            soup = BeautifulSoup(page.content, 'html.parser')
      
            get_name = soup.find_all("title")
            get_type = soup.find_all("div", attrs={"class":"ce"})
            sql = 'insert into fb_events(event_id, event_link) values (%s,%s) returning id;'
            cursor.execute(sql,(k,"facebook.com/events/"+k+"/"))                
            res = cursor.fetchone()[0]
            conn.commit()

            i = 0
            for values in get_details:
                if i%2 == 0:
                    if values.text:
                        event_date_list.append(values.text)
                    else:
                        event_date_list.append("")
                else:
                    if values.text:
                        event_location_list.append(values.text)
                    else:
                        event_location_list.append("")
                i+=1
                
            # for i in range(len(event_date_list)):
            #     if i == event_link.index(k):
            #         sql = 'update fb_events set event_date = %s where event_id =%s and id=%s;'
            #         cursor.execute(sql, (event_date_list[i], k, res))
            #         conn.commit()  
                    

            for i in range(len(event_location_list)):
                if i == event_link.index(k):
                    sql = 'update fb_events set event_location = %s where event_id =%s and id=%s;'
                    cursor.execute(sql, (event_location_list[i], k, res))
                    conn.commit()  
                    
            if get_name:
                for name in get_name:        
                    names.append(name.text)
                    sql = 'update fb_events set event_name = %s where event_id =%s;'
                    cursor.execute(sql, (name.text, k,))
                    conn.commit()
            else:
                names.append("")
                sql = 'update fb_events set event_name = %s where event_id =%s;'
                cursor.execute(sql, ("", k,))
                conn.commit()

            for gtype in get_type:
                gtyp = re.sub('\W+'," ", gtype.text)
                types.append(gtyp)
                sql = 'update fb_events set event_type= %s where event_id =%s;'
                cursor.execute(sql,(gtyp,k,))                
                conn.commit()

            driver = webdriver.Firefox(options=options)
            driver.get('http://facebook.com/events/'+k+'/')
            soup=BeautifulSoup(driver.page_source, features="html.parser")
            driver.close()

            streetInfo = soup.find('script', attrs={'type':'application/ld+json' })
            if streetInfo:
                for i in streetInfo.contents:
                    info = json.loads(i)

                for key, value in info.items():
                    try:
                        startDate = info['startDate']
                    except KeyError:
                        startDate = ""

                    try:
                        endDate = info['endDate']
                    except KeyError:
                        endDate = ""

                    try:
                        streetAddress = info['location']['address']['streetAddress']
                    except KeyError:
                        streetAddress = ""

                    try:
                        postalCode = info['location']['address']['postalCode']
                    except KeyError:
                        postalCode = ""
                    
                    break
            else:
                startDate = endDate = streetAddress = postalCode = ""

            sql = 'update fb_events set startDate = %s,endDate = %s, streetAddress = %s, postalCode = %s\
            where event_id =%s and id=%s;'
            cursor.execute(sql, (startDate, endDate, streetAddress, postalCode, k, res))
            conn.commit()  
            map_url = soup.find('a', attrs={'class':'_42ft _4jy0 _4jy3 _517h _51sy'})
            if map_url:
                driver = webdriver.Firefox(options=options)
                driver.get(map_url['href'])
                soup=BeautifulSoup(driver.page_source, features="html.parser")
                driver.close()

                href = soup.find('script')
                if href:
                    script_tag_contents = href.string
                    wego = re.search(r'("(.+)")', script_tag_contents).group(1)
                    wegohref = wego.replace("\/","/")
                    wegohref = wegohref.replace('"',"")

                    driver = webdriver.Firefox(options=options)
                    driver.get(wegohref)
                    soup=BeautifulSoup(driver.page_source, features="html.parser")
                    driver.close()

                    coords = soup.find_all('script')
                    start = '"customLocation":{'
                    end = ',"zoom"'
                    start_lat = '"latitude":'
                    end_lat = ',"longitude"'
                    start_lon = '"longitude":'
                    end_lon = ''

                    for coord in coords:
                        coord_string = coord.string
                        result = re.search('%s(.*)%s' % (start, end), str(coord_string))
                        if result:
                            lat = re.search('%s(.*)%s' % (start_lat, end_lat), result.group(1)).group(1)
                            lon = re.search('%s(.*)%s' % (start_lon, end_lon), result.group(1)).group(1)
                            sql = 'update fb_events set latitude = %s,longitude = %s where event_id =%s;'
                            cursor.execute(sql,(lat,lon,k,))                
                            conn.commit()
                    latitude.append(lat)
                    longitude.append(lon)
            else:
                latitude.append(None)
                longitude.append(None)
                sql = 'update fb_events set latitude = %s,longitude = %s where event_id =%s;'
                cursor.execute(sql,(None,None,k,))                
                conn.commit()


    ### Load data from eventbrite ###
    driver = webdriver.Firefox(options=options)
    url="https://www.eventbrite.com/d/cyprus/all-events/"
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, features="html.parser")

    all_data = soup.find_all('script')
    start = 'window.__SERVER_DATA__ ='
    end = ';'
    start_date = []
    for data in all_data:
        data_string = data.string
        results = re.findall('%s(.*)%s' % (start, end), str(data_string))
        if results:
            break
    driver.close()

    for k in results:
        result = json.loads(k)

    for i in range(len(result['jsonld'][0])):
        try:
            startDate = result['jsonld'][0][i]['startDate']
        except KeyError:
            startDate = None
            
        try:
            endDate = result['jsonld'][0][i]['endDate']
        except KeyError:
            endDate = None
            
        try:
            event_name = result['jsonld'][0][i]['name']
        except KeyError:
            event_name = None
            
        try:
            event_link = result['jsonld'][0][i]['url']
            event_id = event_link.rsplit('-', 1)[1]
        except KeyError:
            event_link = None        
            
        try:
            streetAddress = result['jsonld'][0][i]['location']['address']['streetAddress']
        except KeyError:
            streetAddress = None
        
        try:
            postalCode = result['jsonld'][0][i]['location']['address']['postalCode']
        except KeyError:
            postalCode = None

        try:
            event_location = result['jsonld'][0][i]['location']['name']
        except KeyError:
            event_location = None        

        try:
            latitude = result['jsonld'][0][i]['location']['geo']['latitude']
        except KeyError:
            latitude = None    
        try:
            longitude = result['jsonld'][0][i]['location']['geo']['longitude']
        except KeyError:
            longitude = None   

        sql = 'select * from eventbrite_events where event_id = %s;'
        cursor.execute(sql,(event_id,))
        row = cursor.fetchall()
        if row:
            continue
        else:    	
            sql = 'insert into eventbrite_events (event_id,event_name,startdate,enddate,\
            streetaddress,postalcode,event_location,event_link,latitude,longitude) \
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
            cursor.execute(sql,(event_id,event_name,startDate,endDate,streetAddress,postalCode,event_location,event_link,latitude,longitude))
            conn.commit()	 

if __name__ == "__main__":
    tl.start(block=True)