import datetime
import psycopg2

from bs4 import BeautifulSoup
from urllib2 import urlopen

BASE_URL = "http://www.nuforc.org/webreports/"
LOC_URL = "ndxloc.html"
DATE_URL = "ndxevent.html"

#get_content = urlopen(BASE_URL).read()
#parse_content = BeautifulSoup(main_stuff, "html5lib")


STATES = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']

STATES_FULL = [u'ALASKA', u'ALABAMA', u'ARKANSAS', u'ARIZONA', u'CALIFORNIA', u'COLORADO', u'CONNECTICUT', u'DELAWARE', u'FLORIDA',
               u'GEORGIA', u'HAWAII', u'IOWA', u'IDAHO', u'ILLINOIS', u'INDIANA', u'KANSAS', u'KENTUCKY', u'LOUISIANA', u'MASSACHUSETTS',
               u'MARYLAND', u'MAINE', u'MICHIGAN', u'MINNESOTA', u'MISSOURI', u'MISSISSIPPI', u'MONTANA', u'NORTH CAROLINA', u'NORTH DAKOTA',
               u'NEBRASKA', u'NEW HAMPSHIRE', u'NEW JERSEY', u'NEW MEXICO', u'NEVADA', u'NEW YORK', u'OHIO', u'OKLAHOMA', u'OREGON',
               u'PENNSYLVANIA', u'RHODE ISLAND', u'SOUTH CAROLINA', u'SOUTH DAKOTA', u'TENNESSEE', u'TEXAS', u'UTAH', u'VIRGINIA',
               u'VERMONT', u'WASHINGTON', u'WISCONSIN', u'WEST VIRGINIA', u'WYOMING']

HEADERS = ['created_at', 'city', 'state', 'shape', 'duration', 'description']

TEMP_SIGHTINGS_SQL = "CREATE TABLE temp_sightings (created TIMESTAMP, city VARCHAR(1024), state VARCHAR(24), shape VARCHAR(1024), description VARCHAR(2048), city_id INT, county_id INT, state_id INT);"


def get_new_connection(dbname, username, password):
    #return psycopg2.connect("dbname={db} user={user} password={pwd}".format(db=dbname, user=username, pwd=password))
    conn = psycopg2.connect("dbname={db} user=postgres".format(db=dbname))
    conn.autocommit = True
    return conn


def update_database(dbname, username, password, start_date, end_date):

    conn = get_new_connection(dbname, username, password)
    with conn.cursor() as cursor:
        cursor.execute("CREATE TABLE places (city VARCHAR(1024), state VARCHAR(256), state_id INT, county_id INT, city_id INT);")
        cursor.execute("INSERT into places (city, state_id, county_id, city_id) SELECT name, state_id, county_id, id FROM cities;")
        cursor.execute("UPDATE places SET state = (SELECT states.code FROM states WHERE states.id = places.state_id);")
        cursor.execute("{}".format(TEMP_SIGHTINGS_SQL))
    conn.close()

    insert_sql = "INSERT INTO temp_sightings (created, city, state, shape, description) values ('{created}', '{city}', '{state}', '{shape}', '{desc}');"
    urls = get_urls_by_date(start_date, end_date)
    for url in urls:
        sightings = get_sightings(url)
        conn = get_new_connection(dbname, username, password)
        with conn.cursor() as cursor:
            for s in sightings:
                cursor.execute(insert_sql.format(created=s[0], city=s[1], state=s[2], shape=[3], desc=s[5]))
        conn.commit()
        conn.close()

    conn = get_new_connection(dbname, username, password)
    with conn.cursor() as cursor:
        # TODO: Delete duplicates
        cursor.execute("UPDATE temp_sightings SET state_id = (SELECT id FROM states WHERE states.code = temp_sightings.state);")
        cursor.execute("UPDATE temp_sightings SET city_id = (SELECT city_id FROM places WHERE places.city = temp_sightings.city and places.state_id = temp_sightings.state_id);")
        cursor.execute("UPDATE temp_sightings SET county_id = (SELECT county_id FROM places WHERE places.city_id = temp_sightings.city_id);")
        cursor.execute("INSERT into sightings (created, description, shape, city_id, state_id, county_id) SELECT created, description, shape, city_id, state_id, county_id FROM temp_sightings WHERE city_id is not null and county_id is not null and state_id is not null;")
    conn.close()


def delete_temp_tables(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("drop table temp_sightings;")
            cursor.execute("drop table places;")
        conn.commit()
        conn.close()
    except psycopg2.ProgrammingError as e:
        print e


def get_urls_by_date(start_date, end_date):  # Send a start_date as "MM/YYYY" or none to get all dates
    index_url = BASE_URL + DATE_URL
    anchors = BeautifulSoup(urlopen(index_url).read(), "html5lib").find('table').find_all('a', href=True)
    urls = []
    for a in anchors:
        try:
            link_date = datetime.datetime.strptime(a.contents[0], "%m/%Y")
            if link_date >= start_date and link_date <= end_date:
                urls.append(BASE_URL + a['href'])
        except ValueError:
            print 'Cannot convert to date %s' % a.contents[0]
            continue
    print 'SD = %s ED = %s' % (start_date, end_date)
    print 'URLS = %s' % urls
    return urls


def get_sightings(url):
    sightings = []
    table = BeautifulSoup(urlopen(url).read(), "html5lib").find('table')
    for tr in table.find_all('tr')[1:]:
        row = []
        for td in tr.find_all('td'):
            row.append(td.text)
        try:
            row.pop()
            row[1] = row[1].split('(')[0].rstrip(' ').replace('"', '').replace('\'', '\'\'').upper().encode('ascii', 'ignore')
            row[2] = row[2].split('(')[0].rstrip(' ').replace('"', '').replace('\'', '\'\'').upper().encode('ascii', 'ignore')
            row[3] = row[3].split('(')[0].rstrip(' ').replace('"', '').replace('\'', '\'\'').upper().encode('ascii', 'ignore')
            row[4] = row[4].split('(')[0].rstrip(' ').replace('"', '').replace('\'', '\'\'').upper().encode('ascii', 'ignore')
            row[5] = row[5].split('(')[0].rstrip(' ').replace('"', '').replace('\'', '\'\'').upper().encode('ascii', 'ignore')
            #  Maybe
            #  row_dict = map(lambda x, y: {x : y}, HEADERS, row)
            #  sightings.append(row_dict)
            sightings.append(row)
        except IndexError:
            print "IndexError with row: %s" % row
            continue
        except ValueError:
            print "Unable to convert first cell to date: %s" % row[0]
            continue
    return sightings


def create_or_append_sightings_db(dbname, username, password, start_date=None, end_date=None, append=True, create_only=False):
    if start_date:
        try:
            start_date = datetime.datetime.strptime(start_date, "%m/%Y")
        except ValueError:
            print 'Invalid start_date: %s' % start_date
            return
    else:
        start_date = datetime.datetime.strptime('01/1950', "%m/%Y")

    if end_date:
        try:
            end_date = datetime.datetime.strptime(end_date, "%m/%Y")
        except ValueError:
            print 'Invalid start_date: %s' % end_date
            return
    else:
        end_date = datetime.datetime.utcnow()

    if append:
        try:
            conn = get_new_connection(dbname, username, password)
            delete_temp_tables(conn)
            update_database(dbname, username, password, start_date, end_date)
            #conn = get_new_connection(dbname, username, password)
            #delete_temp_tables(conn)
        except psycopg2.OperationalError as e:
            print "Cannot connect to database: %s" % dbname
            print "psycopg2 error: %s" % e
            return
    else:
        try:
            admin_conn = get_new_connection("postgres", username, password)
            admin_conn.set_isolation_level(0)
            try:
                conn = get_new_connection(dbname, username, password)
                print 'Database already exists: %s' % dbname
                conn.close()
                drop_db = raw_input('Drop current database (y/N): ')
                if drop_db.upper() == 'Y':
                    confirm = raw_input('Confirm you want to drop the db (y/N): ')
                    if confirm.upper() == 'Y':
                        admin_conn.cursor().execute("drop database %s;" % dbname)
            except psycopg2.OperationalError as e:
                print "This error is probably expected"
                print "psycopg2 error: %s" % e
            print "Creating database..."
            admin_conn.cursor().execute("create database %s;" % dbname)
            admin_conn.close()
            create_db(dbname, username, password)
            if not create_only:
                conn = get_new_connection(dbname, username, password)
                delete_temp_tables(conn)
                update_database(dbname, username, password, start_date, end_date)
                #conn = get_new_connection(dbname, username, password)
                #delete_temp_tables(conn)
        except psycopg2.OperationalError as e:
            print "Can't create a database without admin rights"
            print "psycopg2 error: %s" % e
            return

def create_db(dbname, username, password):
    conn = get_new_connection(dbname, username, password)
    with conn.cursor() as cursor:
        cursor.execute("CREATE TABLE sightings (id serial PRIMARY KEY, created TIMESTAMP, description VARCHAR(2048), shape VARCHAR(256), city_id INT, state_id INT, county_id INT);")
        cursor.execute("CREATE TABLE states (id serial PRIMARY KEY, code VARCHAR(2), name VARCHAR(64), lat REAL, lon REAL);")
        with open('states.csv', 'r') as f:
            cursor.copy_expert(sql="COPY states FROM stdin DELIMITER ',' CSV;", file=f)
    conn.commit()
    conn.close()
    conn = get_new_connection(dbname, username, password)
    with conn.cursor() as cursor:
        cursor.execute("CREATE TABLE counties (id serial PRIMARY KEY, name VARCHAR(256), state_id INT, lat REAL, lon REAL);")
        with open('counties.csv', 'r') as f:
            cursor.copy_expert(sql="COPY counties FROM stdin DELIMITER ',' CSV;", file=f)
    conn.commit()
    conn.close()
    conn = get_new_connection(dbname, username, password)
    with conn.cursor() as cursor:
        cursor.execute("CREATE TABLE cities (id serial PRIMARY KEY, name VARCHAR(1024), state_id INT, county_id INT, lat REAL, lon REAL);")
        with open('cities.csv', 'r') as f:
            cursor.copy_expert(sql="COPY cities FROM stdin DELIMITER ',' CSV;", file=f)
    conn.commit()
    conn.close()

    # original queries to create places table from places.csv
    # ran into problem with duplicate city/state names with different counties

def create_db_places(conn):
    copy_sql = "COPY places FROM stdin DELIMITER ',' CSV;"
    with conn.cursor() as cursor:
        cursor.execute("CREATE TABLE places (city VARCHAR(1024), state VARCHAR(256), county VARCHAR(256), lat REAL, lon REAL);")
        with open('places.csv', 'r') as f:
            cursor.copy_expert(sql=copy_sql, file=f)
    conn.commit()
    conn.close()


def update_db_places(conn):
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM places WHERE county is null;")
        cursor.execute("DELETE FROM places WHERE city is null;")
        cursor.execute("CREATE TABLE states (id serial PRIMARY KEY, code VARCHAR(2), name VARCHAR(64), lat REAL, lon REAL);")
        cursor.execute("CREATE TABLE counties (id serial PRIMARY KEY, name VARCHAR(256), state_id INT, lat REAL, lon REAL);")
        cursor.execute("CREATE TABLE cities (id serial PRIMARY KEY, name VARCHAR(1024), state_id INT, county_id INT, lat REAL, lon REAL);")
        cursor.execute("CREATE TABLE sightings (id serial PRIMARY KEY, created TIMESTAMP, description VARCHAR(2048), shape VARCHAR(256), city_id INT, state_id INT, county_id INT);")
        cursor.execute("ALTER table places add column state_id INT, add column county_id INT, add column city_id INT;")
        cursor.execute("DELETE FROM places WHERE state not in ('AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY');")
        cursor.execute("INSERT into states (code) SELECT distinct(state) FROM places ORDER BY state ASC;")
        cursor.execute("INSERT into counties (name, state_id) SELECT p.county, s.id FROM places p join states s on p.state = s.code GROUP BY s.id, p.county;")
        cursor.execute("UPDATE places SET state_id = (SELECT id FROM states WHERE states.code = places.state);")
        cursor.execute("UPDATE places SET county_id = (SELECT id FROM counties WHERE counties.name = places.county and counties.state_id = places.state_id);")
        cursor.execute("INSERT into cities (name, county_id, state_id, lat, lon) SELECT city, county_id, state_id, avg(lat), avg(lon) FROM places GROUP BY city, county_id, state_id;")
        cursor.execute("UPDATE counties SET lat = (SELECT avg(lat) FROM places WHERE county_id = counties.id GROUP BY county_id);")
        cursor.execute("UPDATE counties SET lon = (SELECT avg(lon) FROM places WHERE county_id = counties.id GROUP BY county_id);")
        cursor.execute("UPDATE states SET lon = (SELECT avg(lon) FROM places WHERE state_id = states.id GROUP BY state_id);")
        cursor.execute("UPDATE states SET lat = (SELECT avg(lat) FROM places WHERE state_id = states.id GROUP BY state_id);")
    conn.commit()
    conn.close()

    ##  Manual stuff
    # create table dubs (name varchar(356), county_name varchar(256), city_id int, state_id int, county_id int, lat real, lon real);
    # select city, state_id, count(*) from places group by city, state_id HAVING count(*) > 1;
    # with duplicates as (select city, state_id, count(*) from places group by city, state_id HAVING count(*) > 1) insert into dubs (name, county_name, city_id, state_id, county_id, lat, lon) select city, county, city_id, state_id, county_id, lat, lon from places where places.city = duplicates.city and places.state_id = duplicates.state_id;
    # delete from dubs where name = county_name;
    # delete from cities where id in (select id from dubs);
    # insert into cities (name, state_id, county_id, lat, lon) select name, state_id, max(county_id), avg(lat), avg(lon) from dubs group by name, state_id;

    # export new tables:
    # copy cities to '/Users/ben/chartio/datasets/ufo/scrapes/cities.csv' delimiter ',' CSV;
    # copy states to '/Users/ben/chartio/datasets/ufo/scrapes/states.csv' delimiter ',' CSV;
    # copy counties to '/Users/ben/chartio/datasets/ufo/scrapes/counties.csv' delimiter ',' CSV;
