import datetime
import dateutil

from bs4 import BeautifulSoup
from urllib2 import urlopen

BASE_URL = "http://www.nuforc.org/webreports/"
LOC_URL = "ndxloc.html"

#main_stuff = urlopen(BASE_URL).read()
#souper = BeautifulSoup(main_stuff, "html5lib")

REAL_PLACES = [u'ALASKA', u'ALABAMA', u'ARKANSAS', u'ARIZONA', u'CALIFORNIA', u'COLORADO', u'CONNECTICUT', u'DELAWARE', u'FLORIDA',
               u'GEORGIA', u'HAWAII', u'IOWA', u'IDAHO', u'ILLINOIS', u'INDIANA', u'KANSAS', u'KENTUCKY', u'LOUISIANA', u'MASSACHUSETTS',
               u'MARYLAND', u'MAINE', u'MICHIGAN', u'MINNESOTA', u'MISSOURI', u'MISSISSIPPI', u'MONTANA', u'NORTH CAROLINA', u'NORTH DAKOTA',
               u'NEBRASKA', u'NEW HAMPSHIRE', u'NEW JERSEY', u'NEW MEXICO', u'NEVADA', u'NEW YORK', u'OHIO', u'OKLAHOMA', u'OREGON',
               u'PENNSYLVANIA', u'RHODE ISLAND', u'SOUTH CAROLINA', u'SOUTH DAKOTA', u'TENNESSEE', u'TEXAS', u'UTAH', u'VIRGINIA',
               u'VERMONT', u'WASHINGTON', u'WISCONSIN', u'WEST VIRGINIA', u'WYOMING']

def get_state_urls(loc_url):
    thing = urlopen(loc_url).read()
    parsed = BeautifulSoup(thing, "html5lib")
    table = parsed.find('table')
    links = []
    for a in table.find_all('a', href=True):
        if a.contents[0] in REAL_PLACES:
            links.append(BASE_URL + a['href'])
    return links

#state_urls = get_state_urls(BASE_URL + LOC_URL)

def get_all_sightings(state_urls):
    sightings = []
    for state in state_urls:
        thing = urlopen(state).read()
        parsed = BeautifulSoup(thing, "html5lib")
        table = parsed.find('table')
        for tr in table.find_all('tr')[1:]:
            row = []
            for td in tr.find_all('td'):
                row.append(td.text)
            #  Don't need the last column
            row.pop()
            try:
                row[0] = datetime.datetime.strptime(row[0], "%m/%d/%y %H:%M")
                sightings.append(row)
            except ValueError:
                #  Don't insert things that can't be dates
                print "Can't make a date with %s" % row[0]
                pass
    return sightings

