DROP TABLE IF EXISTS stations;
DROP TABLE IF EXISTS station_tags;

CREATE TABLE stations(
	id TEXT PRIMARY KEY,
	name TEXT NOT NULL, 
	countrycode TEXT NOT NULL);

CREATE TABLE station_tags(
	id TEXT NOT NULL REFERENCES stations (id),
	tag TEXT NOT NULL,
	PRIMARY KEY (id, tag));

commit;