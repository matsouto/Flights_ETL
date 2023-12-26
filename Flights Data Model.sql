CREATE TABLE "aircrafts_dim" (
  "id" integer PRIMARY KEY,
  "model" varchar(128),
  "aircraft_code" varchar(6),
  "country_code" char(2),
  "airline_id" integer
);

CREATE TABLE "flights_facts" (
  "id" integer PRIMARY KEY,
  "heading" integer,
  "altitude" integer,
  "ground_speed" integer,
  "onground" boolean,
  "latitude" real,
  "logitude" real,
  "origin_airport_id" integer,
  "destination_airport_id" integer,
  "aircraft_id" integer,
  "time_id" integer
);

CREATE TABLE "airports_dim" (
  "id" integer PRIMARY KEY,
  "airport_name" varchar(128) UNIQUE,
  "airport_iata" char(3) UNIQUE,
  "airport_icao" char(4) UNIQUE,
  "airport_latitude" real,
  "airport_longitude" real,
  "region_name" varchar(128),
  "country_code" char(2)
);

CREATE TABLE "airlines_dim" (
  "id" integer PRIMARY KEY,
  "airline_name" varchar(128) UNIQUE,
  "airline_iata" varchar(2) UNIQUE
);

CREATE TABLE "datetimes_dim" (
  "id" integer PRIMARY KEY,
  "time" integer UNIQUE
);

COMMENT ON TABLE "datetimes_dim" IS 'To be improved';

ALTER TABLE "flights_facts" ADD FOREIGN KEY ("origin_airport_id") REFERENCES "airports_dim" ("id") ON DELETE CASCADE;

ALTER TABLE "flights_facts" ADD FOREIGN KEY ("destination_airport_id") REFERENCES "airports_dim" ("id") ON DELETE CASCADE;

ALTER TABLE "aircrafts_dim" ADD FOREIGN KEY ("id") REFERENCES "flights_facts" ("aircraft_id") ON DELETE CASCADE;

ALTER TABLE "flights_facts" ADD FOREIGN KEY ("time_id") REFERENCES "datetimes_dim" ("id");

ALTER TABLE "aircrafts_dim" ADD FOREIGN KEY ("airline_id") REFERENCES "airlines_dim" ("id");
