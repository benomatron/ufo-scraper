import datetime
import psycopg2

from bs4 import BeautifulSoup
from urllib2 import urlopen

BASE_URL = "http://www.nuforc.org/webreports/"
LOC_URL = "ndxloc.html"
DATE_URL = "ndxevent.html"

#main_stuff = urlopen(BASE_URL).read()
#souper = BeautifulSoup(main_stuff, "html5lib")


STATES = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']

REAL_PLACES = [u'ALASKA', u'ALABAMA', u'ARKANSAS', u'ARIZONA', u'CALIFORNIA', u'COLORADO', u'CONNECTICUT', u'DELAWARE', u'FLORIDA',
               u'GEORGIA', u'HAWAII', u'IOWA', u'IDAHO', u'ILLINOIS', u'INDIANA', u'KANSAS', u'KENTUCKY', u'LOUISIANA', u'MASSACHUSETTS',
               u'MARYLAND', u'MAINE', u'MICHIGAN', u'MINNESOTA', u'MISSOURI', u'MISSISSIPPI', u'MONTANA', u'NORTH CAROLINA', u'NORTH DAKOTA',
               u'NEBRASKA', u'NEW HAMPSHIRE', u'NEW JERSEY', u'NEW MEXICO', u'NEVADA', u'NEW YORK', u'OHIO', u'OKLAHOMA', u'OREGON',
               u'PENNSYLVANIA', u'RHODE ISLAND', u'SOUTH CAROLINA', u'SOUTH DAKOTA', u'TENNESSEE', u'TEXAS', u'UTAH', u'VIRGINIA',
               u'VERMONT', u'WASHINGTON', u'WISCONSIN', u'WEST VIRGINIA', u'WYOMING']

HEADERS = ['created_at', 'city', 'state', 'shape', 'duration', 'description']

TEMP_SIGHTINGS_SQL = "CREATE TABLE temp_sightings (id serial PRIMARY KEY, created TIMESTAMP, city VARCHAR(1024), state VARCHAR(24), shape VARCHAR(1024), description VARCHAR(2048), city_id INT, county_id INT, state_id INT);"

def get_sub_urls(loc_url, filter_set=None, start_date=None):
    """ Send a filter set for states
        Send a start_date as "MM/YYYY" for event dates
        If you are looking to retrieve non sequential months use a filter set for event dates
    """
    if start_date:
        try:
            start = datetime.datetime.strptime(start_date, "%m/%Y")
        except ValueError:
            print 'Invalid start_date: %s' % start_date
            return
    anchors = BeautifulSoup(urlopen(loc_url).read(), "html5lib").find('table').find_all('a', href=True)
    sub_urls = []
    for a in anchors:
        if not filter_set or a.contents[0] in filter_set:
            if start_date:
                try:
                    link_date = datetime.datetime.strptime(a.contents[0], "%m/%Y")
                    if link_date >= start:
                        sub_urls.append(BASE_URL + a['href'])
                except ValueError:
                    print 'Count not convert to date %s' % a.contents[0]
                    continue
            else:
                sub_urls.append(BASE_URL + a['href'])
    return sub_urls


def create_sightings_db(db_name):
    conn = psycopg2.connect("dbname=postgres user=postgres" % dbname)
    conn.set_isolation_level(0)
    conn.cursor().execute("create database %s" % dbname)


def prepare_sightings_db(dbname):
    conn = psycopg2.connect("dbname=postgres user=postgres" % dbname)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE places (city VARCHAR(1024), state VARCHAR(256), state_id INT, county_id INT, city_id INT);")
    cursor.execute("insert into places (city, state_id, county_id, city_id) select name, state_id, county_id, id from cities;")
    cursor.execute("update places set state = (select states.code from states where states.id = places.state_id);")
    cursor.execute("{}".format(TEMP_SIGHTINGS_SQL))


def create_new_entries(dbname):
    """
    """


def update_new_entries(dbname):
    conn = psycopg2.connect("dbname=postgres user=postgres" % dbname)
    cursor = conn.cursor()
    cursor.execute("update temp_sightings set state_id = (select state_id from places where places.state = temp_sightings.state);")
    cursor.execute("update temp_sightings set city_id = (select city_id from places where places.city = temp_sightings.city and places.state_id = temp_sightings.state_id);")
    cursor.execute("update temp_sightings set county_id = (select county_id from places where places.city_id = temp_sightings.city_id);")
    cursor.execute("insert into sightings (created, description, shape, city_id, state_id, county_id) select created, description, shape, city_id, state_id, county_id from temp_sightings where city_id is not null and county_id is not null and state_id is not null;")

def cleanup_database(dbname):
    conn = psycopg2.connect("dbname=postgres user=postgres" % dbname)
    cursor = conn.cursor()
    cursor.execute("drop table temp_sightings;")
    cursor.execute("drop table places;")

def can_connect_postgres():
    try:
        conn = psycopg2.connect("dbname=postgres user=postgres")
        conn.cursor.execute("select * from postgres limit 1;")
        return True
    except psycopg2.OperationalError as e:
        print "psycopg2 error: %s" % e
        return False


def sexy_sightings(dbname,  append=True, create_db=False):

    if append:
        try:
            conn = psycopg2.connect("dbname=%s user=postgres" % dbname)
            cursor = conn.cursor()
            prepare_sightings_db(dbname)
            for url in them_urls:
                create_new_entries(dbname)
            update_new_entries(dbname)
            cleanup_database(dbname)
        except psycopg2.OperationalError as e:
            print "Cannot connect to database: %s" % dbname
            print "psycopg2 error: %s" % e
            return
    elif create_db:
        try:
            test_conn = psycopg2.connect("dbname=%s user=postgres" % dbname)
            print 'Database already exists: %s' % dbname
            drop_db = raw_input('Drop current database (y/N): ')
            if drop_db.upper() == 'Y':
                confirm = raw_input('Confirm you want to drop the db (y/N): ')
                if confirm.upper() == 'Y':
                    conn = psycopg2.connect("dbname=postgres user=postgres")
                    conn.set_isolation_level(0)
                    conn.cursor().execute("drop database %s;" % dbname)
                    conn.commit()
                    conn.close()
                    create_sightings_db(dbname)
        except psycopg2.OperationalError as e:
            print "psycopg2 error: %s" % e


def get_sightings(url_list, dbname):
    sightings = []
    for url in url_list:
        table = BeautifulSoup(urlopen(url).read(), "html5lib").find('table')
        for tr in table.find_all('tr')[1:]:
            row = []
            for td in tr.find_all('td'):
                row.append(td.text)
            try:  # Clean up and validate
                row.pop()
                row[0] = datetime.datetime.strptime(row[0], "%m/%d/%y %H:%M")
                row[1] = row[1].split('(')[0].rstrip(' ')
                row[2] = row[2].split('(')[0].rstrip(' ')
                #  Maybe
                #row_dict = map(lambda x, y: {x : y}, HEADERS, row)
                #sightings.append(row_dict)
                sightings.append(row)
            except IndexError:
                print "IndexError with row: %s" % row
                continue
            except ValueError:
                print "Unable to convert first cell to date: %s" % row[0]
                continue
    return sightings


"""
-- initial queries to create places tables from csv
create database ufo_sightings;
\c ufo_sightings
CREATE TABLE places (city VARCHAR(1024), state VARCHAR(256), county VARCHAR(256), lat REAL, lon REAL);
CREATE TABLE states (id serial PRIMARY KEY, code VARCHAR(2), name VARCHAR(64), lat REAL, lon REAL);
CREATE TABLE counties (id serial PRIMARY KEY, name VARCHAR(256), state_id INT, lat REAL, lon REAL);
CREATE TABLE cities (id serial PRIMARY KEY, name VARCHAR(1024), state_id INT, county_id INT, class VARCHAR(64), lat REAL, lon REAL);
CREATE TABLE sightings (id serial PRIMARY KEY, created TIMESTAMP, description VARCHAR(2048), shape VARCHAR(256), city_id INT, state_id INT, county_id INT);

COPY places FROM '/Users/ben/chartio/datasets/ufo/scrapes/places.csv' DELIMITER ',' CSV;
alter table places add column state_id INT, add column county_id INT, add column city_id INT;
DELETE FROM places where state not in ('AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY');")

insert into states (code) select distinct(state) from places order by state asc;
insert into counties (name, state_id) select p.county, s.id from places p join states s on p.state = s.code group by s.id, p.county;
update places set state_id = (select id from states where states.code = places.state);
update places set county_id = (select id from counties where counties.name = places.county and counties.state_id = places.state_id);
insert into cities (name, county_id, state_id, lat, lon) select city, county_id, state_id, avg(lat), avg(lon) from places group by city, county_id, state_id;
update counties set lat = (select avg(lat) from places where county_id = counties.id group by county_id);
update counties set lon = (select avg(lon) from places where county_id = counties.id group by county_id);
update states set lon = (select avg(lon) from places where state_id = states.id group by state_id);
update states set lat = (select avg(lat) from places where state_id = states.id group by state_id);
DROP TABLE places;
"""
