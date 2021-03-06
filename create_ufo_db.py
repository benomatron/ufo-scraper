import datetime
import psycopg2

from bs4 import BeautifulSoup
from urllib2 import urlopen


BASE_URL = "http://www.nuforc.org/webreports/"
DATE_URL = "ndxevent.html"


SHAPES = ['Changing', 'Chevron', 'Cigar', 'Circle', 'Cone', 'Cross', 'Cylinder', 'Diamond', 'Disk', 'Egg', 'Fireball', 'Flash',
          'Formation', 'Light', 'Other', 'Oval', 'Rectangle', 'Sphere', 'Teardrop', 'Triangle']

NUMBER_WORDS = {'one': 1, 'two': 2,  'three': 3, 'four': 4, 'five': 5, 'six': 6, 'seven': 7, 'eight': 8, 'nine': 9,
                'ten': 10, 'eleven': 11,'twelve': 12, 'thirteen': 13, 'fourteen': 14,'fifteen': 15, 'sixteen': 16,
                'seventeen': 17, 'eighteen': 18, 'nineteen': 19, 'twenty':  20, 'thirty': 30, 'forty': 40, 'fifty': 50,
                'sixty': 60, 'seventy': 70, 'eighty': 80, 'ninety': 90}

TEMP_SIGHTINGS_SQL = "CREATE TABLE temp_sightings (created_date TIMESTAMP, city VARCHAR(1024), state VARCHAR(24), shape VARCHAR(1024), duration int, description VARCHAR(2048), city_id INT, county_id INT, state_id INT);"

MIN_DATE = datetime.datetime(2000, 1, 1)
MAX_DATE = datetime.datetime.utcnow()


def get_new_connection(dbname, username, password):
    #return psycopg2.connect("dbname={db} user={user} password={pwd}".format(db=dbname, user=username, pwd=password))
    conn = psycopg2.connect("dbname={db} user=postgres".format(db=dbname))
    conn.autocommit = True
    return conn


def update_database(conn, start_date, end_date):

    with conn.cursor() as cursor:
        cursor.execute("CREATE TABLE places (city VARCHAR(1024), state VARCHAR(256), state_id INT, county_id INT, city_id INT);")
        cursor.execute("INSERT into places (city, state_id, county_id, city_id) SELECT name, state_id, county_id, id FROM cities;")
        cursor.execute("UPDATE places SET state = (SELECT states.code FROM states WHERE states.id = places.state_id);")
        cursor.execute("{}".format(TEMP_SIGHTINGS_SQL))

    insert_sql = "INSERT INTO temp_sightings (created_date, city, state, shape, duration, description) values ('{created}', '{city}', '{state}', '{shape}', '{dur}', '{desc}');"
    urls = get_urls_by_date(start_date, end_date)
    for url in urls:
        sightings = get_sightings(url)
        print 'Inserting to temp_sightings %s' % url
        with conn.cursor() as cursor:
            for s in sightings:
                cursor.execute(insert_sql.format(created=s[0], city=s[1], state=s[2], shape=s[3], dur=s[4], desc=s[5]))

    print 'Updating sightings from temp_sightings'
    with conn.cursor() as cursor:
        cursor.execute("UPDATE temp_sightings SET state_id = (SELECT id FROM states WHERE states.code = temp_sightings.state);")
        cursor.execute("UPDATE temp_sightings SET city_id = (SELECT city_id FROM places WHERE places.city = temp_sightings.city and places.state_id = temp_sightings.state_id);")
        cursor.execute("UPDATE temp_sightings SET county_id = (SELECT county_id FROM places WHERE places.city_id = temp_sightings.city_id);")
        cursor.execute("DELETE FROM temp_sightings WHERE city_id IS NULL OR county_id IS NULL OR state_id IS NULL;")
        cursor.execute("INSERT into sightings (created_date, shape, duration, description, city_id, state_id, county_id) SELECT created_date, shape, duration, description, city_id, state_id, county_id FROM temp_sightings;")
        cursor.execute("DELETE FROM sightings WHERE created_date < '{mindate}' OR created_date > '{maxdate}';".format(mindate=datetime.datetime.strftime(MIN_DATE, "%Y/%m/%d"), maxdate=datetime.datetime.strftime(MAX_DATE, "%Y/%m/%d")))
        cursor.execute("CREATE TABLE dup_sightings (id INT, created_date TIMESTAMP, shape VARCHAR(256), duration int, description VARCHAR(2048), city_id INT, state_id INT, county_id INT);")
        cursor.execute("WITH duples AS (SELECT created_date, shape, duration, description, city_id, state_id, county_id from sightings GROUP BY created_date, shape, duration, description, city_id, state_id, county_id HAVING count(*) > 1) INSERT INTO dup_sightings (id, created_date, shape, duration, description, city_id, state_id, county_id) SELECT s.id, s.created_date, s.shape, s.duration, s.description, s.city_id, s.state_id, s.county_id FROM sightings s JOIN duples d ON s.created_date = d.created_date AND s.shape = d.shape AND s.duration = d.duration AND s.description = d.description AND s.city_id = d.city_id AND s.state_id = d.state_id AND s.county_id = d.county_id;")
        cursor.execute("DELETE FROM sightings WHERE id in (select id from dup_sightings);")
        cursor.execute("INSERT INTO sightings (created_date, shape, duration, description, city_id, state_id, county_id) SELECT created_date, shape, duration, description, city_id, state_id, county_id FROM dup_sightings GROUP BY created_date, shape, duration, description, city_id, state_id, county_id;")


def delete_temp_tables(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS temp_sightings;")
            cursor.execute("DROP TABLE IF EXISTS places;")
            cursor.execute("DROP TABLE IF EXISTS dup_sightings;")
    except psycopg2.ProgrammingError as e:
        print e


def get_urls_by_date(start_date, end_date):
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


def time_multi(string):
    time_val = 0
    if string.lower().__contains__('sec'):
        time_val += 1
    if string.lower().__contains__('min'):
        time_val += 60
    if string.lower().__contains__('hour'):
        time_val += 3600
    if string.lower().__contains__('day'):
        time_val += 86400
    return max(time_val, 1)


def first_int(string):
    x = ''
    #  Get first non broken number
    for s in string:
        if s.isdigit():
            x += s
        elif x:
            break
    if len(x):
        return int(x)


def guess_number(string):
    val = 0
    words = [k for k, v in NUMBER_WORDS.iteritems()]
    for word in words:
        if string.__contains__(word):
            x = NUMBER_WORDS[word]
            val = x if x > val else val
    return val


def get_sightings(url):
    print 'Getting url %s' % url
    sightings = []
    table = BeautifulSoup(urlopen(url).read(), "html5lib").find('table')
    row_count = 0
    for tr in table.find_all('tr')[1:]:
        row = []
        row_count += 1
        if not row_count % 100:
            print row_count
        for td in tr.find_all('td'):
            row.append(td.text)
        try:
            #  City
            row[1] = row[1].split('(')[0].split('/')[0].rstrip(' ').replace('"', '').replace('\'', '').replace('-', ' ').replace('.', '').upper().encode('ascii', 'ignore')
            #  Most common city naming problems
            if row[1].startswith('ST '):
                row[1] = 'SAINT ' + row[1].lstrip('ST ')
            elif row[1] in ('NEW YORK CITY', 'NYC'):
                row[1] = 'NEW YORK'
            elif row[1] in ('WASHINGTON, DC', 'WASHINGTON DC'):
                row[1] = 'WASHINGTON'

            #  State
            row[2] = row[2].split('(')[0].rstrip(' ').replace('"', '').replace('\'', '').replace('.', '').upper().encode('ascii', 'ignore')

            #  Shape
            row[3] = row[3].split('(')[0].rstrip(' ').replace('"', '').replace('\'', '').replace('.', '').capitalize().encode('ascii', 'ignore')
            if row[3] not in SHAPES:
                row[3] = 'Other'

            #  Duration
            row[4] = row[4].split('(')[0].rstrip(' ').replace('"', '').replace('\'', '').lower().encode('ascii', 'ignore')

            multiple = time_multi(row[4])
            num = first_int(row[4])
            if not num:
                num = guess_number(row[4])
            if num:
                num = num * multiple
            else:
                num = 10
            row[4] = num

            #  Description
            row[5] = row[5].split('(')[0].rstrip(' ').replace('"', '').replace('\'', '').capitalize().encode('ascii', 'ignore')

            #  Drop the last column
            row.pop()

            sightings.append(row)
        except IndexError:
            print "IndexError with row: %s" % row
            continue
        except ValueError:
            print "Unable to convert first cell to date: %s" % row[0]
            continue
    return sightings


# create_or_append_sightings_db("ufos", "user", "pass", "01/2000", "03/2000", append=False, create_only=False)
def create_or_append_sightings_db(dbname, username, password, start_date=None, end_date=None, append=True, create_only=False):
    if start_date:
        try:
            start_date = datetime.datetime.strptime(start_date, "%m/%Y")
            start_date = MIN_DATE if start_date < MIN_DATE else start_date
        except ValueError:
            print 'Invalid start_date: %s' % start_date
            return
    else:
        start_date = MIN_DATE

    if end_date:
        try:
            end_date = datetime.datetime.strptime(end_date, "%m/%Y")
            end_date = MAX_DATE if end_date > MAX_DATE else end_date
        except ValueError:
            print 'Invalid start_date: %s' % end_date
            return
    else:
        end_date = MAX_DATE

    if append:
        try:
            conn = get_new_connection(dbname, username, password)
            delete_temp_tables(conn)
            update_database(conn, start_date, end_date)
            delete_temp_tables(conn)
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
                print "This error is probably expected..."
                print "psycopg2 error: %s" % e
            print "Creating database..."
            admin_conn.cursor().execute("create database %s;" % dbname)
            admin_conn.close()
            conn = get_new_connection(dbname, username, password)
            create_initial_db(conn)
            if not create_only:
                delete_temp_tables(conn)
                update_database(conn, start_date, end_date)
                delete_temp_tables(conn)
        except psycopg2.OperationalError as e:
            print "Can't create a database without admin rights"
            print "psycopg2 error: %s" % e
            return


def create_initial_db(conn):
    with conn.cursor() as cursor:
        cursor.execute("CREATE TABLE sightings (id serial PRIMARY KEY, created_date TIMESTAMP, shape VARCHAR(256), duration int, description VARCHAR(2048), city_id INT, state_id INT, county_id INT);")
        cursor.execute("CREATE TABLE states (id serial PRIMARY KEY, code VARCHAR(2), name VARCHAR(64), lat REAL, lon REAL);")
        cursor.execute("CREATE TABLE counties (id serial PRIMARY KEY, name VARCHAR(256), state_id INT, lat REAL, lon REAL);")
        cursor.execute("CREATE TABLE cities (id serial PRIMARY KEY, name VARCHAR(1024), state_id INT, county_id INT, lat REAL, lon REAL);")

        with open('states.csv', 'r') as f:
            cursor.copy_expert(sql="COPY states FROM stdin DELIMITER ',' CSV;", file=f)
        with open('counties.csv', 'r') as f:
            cursor.copy_expert(sql="COPY counties FROM stdin DELIMITER ',' CSV;", file=f)
        with open('cities.csv', 'r') as f:
            cursor.copy_expert(sql="COPY cities FROM stdin DELIMITER ',' CSV;", file=f)

        cursor.execute("ALTER TABLE sightings ADD CONSTRAINT fk_city FOREIGN KEY (city_id) REFERENCES cities ON DELETE CASCADE;")
        cursor.execute("ALTER TABLE counties ADD CONSTRAINT fk_state FOREIGN KEY (state_id) REFERENCES states ON DELETE CASCADE;")
        cursor.execute("ALTER TABLE cities ADD CONSTRAINT fk_state FOREIGN KEY (state_id) REFERENCES states ON DELETE CASCADE;")
        cursor.execute("ALTER TABLE cities ADD CONSTRAINT fk_county FOREIGN KEY (county_id) REFERENCES counties ON DELETE CASCADE;")
