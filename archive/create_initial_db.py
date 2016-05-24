#import psycopg2
#
#def create_initial_db(conn):
#    with conn.cursor() as cursor:
#        cursor.execute("CREATE TABLE sightings (id serial PRIMARY KEY, created_date TIMESTAMP, shape VARCHAR(256), duration int, description VARCHAR(2048), city_id INT, state_id INT, county_id INT);")
#        cursor.execute("CREATE TABLE states (id serial PRIMARY KEY, code VARCHAR(2), name VARCHAR(64), lat REAL, lon REAL);")
#        cursor.execute("CREATE TABLE counties (id serial PRIMARY KEY, name VARCHAR(256), state_id INT, lat REAL, lon REAL);")
#        cursor.execute("CREATE TABLE cities (id serial PRIMARY KEY, name VARCHAR(1024), state_id INT, county_id INT, lat REAL, lon REAL);")
#
#        with open('states.csv', 'r') as f:
#            cursor.copy_expert(sql="COPY states FROM stdin DELIMITER ',' CSV;", file=f)
#        with open('counties.csv', 'r') as f:
#            cursor.copy_expert(sql="COPY counties FROM stdin DELIMITER ',' CSV;", file=f)
#        with open('cities.csv', 'r') as f:
#            cursor.copy_expert(sql="COPY cities FROM stdin DELIMITER ',' CSV;", file=f)
#
#        cursor.execute("ALTER TABLE sightings ADD CONSTRAINT fk_city FOREIGN KEY (city_id) REFERENCES cities ON DELETE CASCADE;")
#        cursor.execute("ALTER TABLE counties ADD CONSTRAINT fk_state FOREIGN KEY (state_id) REFERENCES states ON DELETE CASCADE;")
#        cursor.execute("ALTER TABLE cities ADD CONSTRAINT fk_state FOREIGN KEY (state_id) REFERENCES states ON DELETE CASCADE;")
#        cursor.execute("ALTER TABLE cities ADD CONSTRAINT fk_county FOREIGN KEY (county_id) REFERENCES counties ON DELETE CASCADE;")
#
#
###  -- build unique cities, counties, and states from places.csv
#STATES = {'AK': 'ALASKA', 'AL': 'ALABAMA', 'AR': 'ARKANSAS', 'AZ': 'ARIZONA', 'CA': 'CALIFORNIA', 'CO': 'COLORADO',
#          'CT': 'CONNECTICUT', 'DC': 'DISTRICT OF COLUMBIA', 'DE': 'DELAWARE', 'FL': 'FLORIDA', 'GA': 'GEORGIA',
#          'HI': 'HAWAII', 'IA': 'IOWA', 'ID': 'IDAHO', 'IL': 'ILLINOIS', 'IN': 'INDIANA', 'KS': 'KANSAS', 'KY': 'KENTUCKY',
#          'LA': 'LOUISIANA', 'MA': 'MASSACHUSETTS', 'MD': 'MARYLAND', 'ME': 'MAINE', 'MI': 'MICHIGAN', 'MN': 'MINNESOTA',
#          'MO': 'MISSOURI', 'MS': 'MISSISSIPPI', 'MT': 'MONTANA', 'NC': 'NORTH CAROLINA', 'ND': 'NORTH DAKOTA', 'NE': 'NEBRASKA',
#          'NH': 'NEW HAMPSHIRE', 'NJ': 'NEW JERSEY', 'NM': 'NEW MEXICO', 'NV': 'NEVADA', 'NY': 'NEW YORK', 'OH': 'OHIO',
#          'OK': 'OKLAHOMA', 'OR': 'OREGON', 'PA': 'PENNSYLVANIA', 'RI': 'RHODE ISLAND', 'SC': 'SOUTH CAROLINA',
#          'SD': 'SOUTH DAKOTA', 'TN': 'TENNESSEE', 'TX': 'TEXAS', 'UT': 'UTAH', 'VA': 'VIRGINIA', 'VT': 'VERMONT',
#          'WA': 'WASHINGTON', 'WI': 'WISCONSIN', 'WV': 'WEST VIRGINIA', 'WY': 'WYOMING'}
#
# update_state_sql = "UPDATE states SET name = '{name}' WHERE code = '{code}';"
#        for k, v in STATES.iteritems():
#            cursor.execute(update_state_sql.format(name=v, code=k))

# CREATE TABLE places (city VARCHAR(1024), state VARCHAR(256), county VARCHAR(256), lat REAL, lon REAL);
# CREATE TABLE state_loc (code VARCHAR(2), lat REAL, lon REAL);
# COPY places FROM '/Users/ben/chartio/datasets/ufo/scrapes/places.csv' DELIMITER ',' CSV;
# COPY state_loc FROM '/Users/ben/chartio/datasets/ufo/scrapes/state_loc.csv' DELIMITER ',' CSV;

# DELETE FROM places WHERE county is null;
# DELETE FROM places WHERE city is null;
# DELETE FROM places WHERE state not in ('AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY');
# ALTER table places add column state_id INT, add column county_id INT, add column city_id INT;
# CREATE TABLE states (id serial PRIMARY KEY, code VARCHAR(2), name VARCHAR(64), lat REAL, lon REAL);
# CREATE TABLE counties (id serial PRIMARY KEY, name VARCHAR(256), state_id INT, lat REAL, lon REAL);
# CREATE TABLE cities (id serial PRIMARY KEY, name VARCHAR(1024), state_id INT, county_id INT, lat REAL, lon REAL);
# INSERT into states (code) SELECT distinct(state) FROM places ORDER BY state ASC;
# INSERT into counties (name, state_id) SELECT p.county, s.id FROM places p join states s on p.state = s.code GROUP BY s.id, p.county;
# UPDATE places SET state_id = (SELECT id FROM states WHERE states.code = places.state);
# UPDATE places SET county_id = (SELECT id FROM counties WHERE counties.name = places.county and counties.state_id = places.state_id);
# INSERT into cities (name, county_id, state_id, lat, lon) SELECT city, county_id, state_id, avg(lat), avg(lon) FROM places GROUP BY city, county_id, state_id;
# UPDATE counties SET lat = (SELECT avg(lat) FROM places WHERE county_id = counties.id GROUP BY county_id);
# UPDATE counties SET lon = (SELECT avg(lon) FROM places WHERE county_id = counties.id GROUP BY county_id);
# UPDATE cities SET lat = (select lat from counties where counties.id = cities.county_id) WHERE lat is null;
# UPDATE cities SET lon = (select lon from counties where counties.id = cities.county_id) WHERE lon is null;
# UPDATE states SET lat = (select lat from state_loc where states.code = state_loc.code);
# UPDATE states SET lon = (select lon from state_loc where states.code = state_loc.code);


###  -- dedupe cities
# CREATE TABLE doubles (id int, name VARCHAR(1024), state_id INT, county_id INT, county_name varchar(256), lat REAL, lon REAL);

# WITH duplicates AS (select name, state_id, count(*) from cities group by name, state_id HAVING count(*) > 1)
# INSERT INTO doubles (id, name, state_id, county_id, lat, lon)
# SELECT c.id, c.name, c.state_id, c.county_id, c.lat, c.lon FROM cities c
# JOIN duplicates d ON c.name = d.name AND c.state_id = d.state_id;

# UPDATE doubles SET county_name = (select name from counties where counties.id = doubles.county_id);
# CREATE TABLE city_counties (id int, city_name varchar(256), county_name varchar(256));
# INSERT INTO city_counties (id, city_name, county_name) select id, name, county_name from doubles where name = county_name;
# DELETE FROM doubles WHERE id in (select id from city_counties);

# DELETE FROM cities WHERE id in (select id from doubles);
# DELETE FROM doubles WHERE name in (select city_name from city_counties);

###  -- some approximation of cities that span multiple counties
# INSERT INTO cities (name, state_id, county_id, lat, lon) SELECT name, state_id, MAX(county_id), AVG(lat), AVG(lon) FROM doubles GROUP BY name, state_id;
# DROP TABLE city_counties;
# DROP TABLE doubles;
# DROP TABLE places;
# DROP TABLE state_loc;
###  -- export
# COPY cities TO '/Users/ben/chartio/datasets/ufo/scrapes/cities.csv' DELIMITER',' CSV;
# COPY states TO '/Users/ben/chartio/datasets/ufo/scrapes/states.csv' DELIMITER ',' CSV;
# COPY counties TO '/Users/ben/chartio/datasets/ufo/scrapes/counties.csv' DELIMITER ',' CSV;
