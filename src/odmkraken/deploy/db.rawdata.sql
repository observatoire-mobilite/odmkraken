{% set EVENT_TYPE_CODES = {
    -1: 'ping', 
    0: 'geplante Haltestelle + Fahrplanpunkt',
    1: 'Bedarfshaltestelle (geplant)',
    2: 'ungeplante Haltestelle + Tür offen',
    3: 'Störungspunkt',
    4: 'Durchfahrt ohne Fahrgastaufnahme',
    5: 'Haltestelle + kein Fahrplanpunkt',
    6: 'Durchfahrt ohne Fahrgastaufnahme oder Fahrplanpunkt'}
%}

/* enum encoding possible types of events */
create type {{vehdata_schema}}."event_type" as enum (
    {%- for value in EVENT_TYPE_CODES.values() %}
    {{value | li}}{% if not loop.last %},{% endif %}
    {%- endfor %}
);
comment on type {{vehdata_schema}}."event_type" is 'All possible types of messages received from vehicles.';

/* function to encode init codes to the string version of the enum */
create or replace function {{vehdata_schema}}."event_code_to_event_type"(code integer)
returns {{vehdata_schema}}."event_type" as $$
declare
    answer {{vehdata_schema}}."event_type";
begin
    case code
        {%- for code, value in EVENT_TYPE_CODES.items() %}
        when {{code |li}} then answer := {{value | li}};
        {%- endfor -%}
        else
            raise exception 'invalid input --> %', integer
                USING HINT = 'acceptable values are: {% for code in EVENT_TYPE_CODES.keys()%}{{code}}{% if not loop.last %},{% endif %}{% endfor %}';
    end case;
    return answer; 
end;
$$ language plpgsql;
comment on function {{vehdata_schema}}."event_code_to_event_type" is 'Convert integer representation of categorical `kind` variable back to textual form, encoded by the enum `event_type`.';

/* reverse: from string version of the enum to codes */
create or replace function {{vehdata_schema}}."event_type_to_event_code"(event {{vehdata_schema}}."event_type")
returns integer as $$
declare
    answer integer;
begin
    case event
        {%- for code, value in EVENT_TYPE_CODES.items() %}
        when {{value |li}} then answer := {{code |li}};
        {%- endfor %}
        else
            raise exception 'invalid input --> %', integer
                USING HINT = 'acceptable values are: {% for code in EVENT_TYPE_CODES.values()%}{{code}}{% if not loop.last %},{% endif %}{% endfor %}';
    end case;
    return answer; 
end;
$$ language plpgsql;
comment on function {{vehdata_schema}}."event_type_to_event_code" is 'Convert textual representation of categorical `kind` variable back to integer.';

/* lines table: known line services of public transport */
create table if not exists {{vehdata_schema}}.lines (
    id serial primary key,
    code varchar unique not null,
    first_seen timestamp
);
comment on table {{vehdata_schema}}.lines is 'Known line services of public transport.';
comment on column {{vehdata_schema}}.lines.id is 'Internal identifier, uniquely identifying every known line service; assigned while importing on first occurence in the data (primary key).';
comment on column {{vehdata_schema}}.lines.code is 'Identifier assigned by the tracking system, uniquely identifying a given line service on a given day.';
comment on column {{vehdata_schema}}.lines.first_seen is 'Time and date of first detection of this line in the data.';

/* vehicles table: known vehicles on the network */
create table if not exists {{vehdata_schema}}.vehicles (
    id serial primary key,
    code integer,
    plate varchar check (plate ~ '([A-Z]*\d+|[a-zA-Z0-9 ]+)'),
    first_seen timestamp,
    original_code varchar,
    unique(code, plate)
);
comment on table {{vehdata_schema}}.vehicles is 'Known vehicles on the network.';
comment on column {{vehdata_schema}}.vehicles.id is 'Internal identifier, uniquely identifying every known vehicle; assigned while importing, on first occurence in the data (primary key).';
comment on column {{vehdata_schema}}.vehicles.code is 'Numeric portion of the vehicle identifier used by the tracking system. Note: the combination of `code` and `plate` has to be unique.';
comment on column {{vehdata_schema}}.vehicles.plate is 'Licence plate number of the vehicle or alpha-numeric part of the code used by the tracking system. Note: the combination of `code` and `plate` has to be unique.';
comment on column {{vehdata_schema}}.vehicles.first_seen is 'Time and date of first detection of this vehicle in the data.';

/* stops table: known stops on the network */
create table if not exists {{vehdata_schema}}.stops (
    id serial primary key,
    code varchar unique,
    first_seen timestamp
);
comment on table {{vehdata_schema}}.stops is 'Known stops on the network.';
comment on column {{vehdata_schema}}.stops.id is 'Internal identifier, uniquely identifying every known stop; assigned while importing, on first occurence in the data (primary key).';
comment on column {{vehdata_schema}}.stops.code is 'Identifier assigned by the tracking system, uniquely identifying a given stop on a given day.';
comment on column {{vehdata_schema}}.stops.first_seen is 'Time and date of first detection of this stop in the data.';

/* runs table: service state of a vehicle at a given time */
create table if not exists {{vehdata_schema}}.runs (
    vehicle_id integer references {{vehdata_schema}}.vehicles(id),
    line_id integer references {{vehdata_schema}}.lines(id),
    sortie integer,
    sortie_flag text,
    run integer,
    time_start timestamp,
    time_end timestamp,
    primary key (vehicle_id, time_start, time_end)
);
comment on table {{vehdata_schema}}.runs is 'Runs performed by vehicles: period where the vehicle displays the same combination of line_id, sortie and run over a continguous sequence of messages. Primary key: vehicle_id, time_start and time_end';
comment on column {{vehdata_schema}}.runs.vehicle_id is 'Identity of the vehicle';
comment on column {{vehdata_schema}}.runs.line_id is 'Line service the vehicle reported performing. May be null, indicating: no line displayed.';
comment on column {{vehdata_schema}}.runs.sortie is 'Numerical identifier of the pre-programmed course. If 0, the vehicle is not operating as a vehicle of public transport.';
comment on column {{vehdata_schema}}.runs.sortie_flag is 'Weird textual prefix that comes with some `UMLAUF`.';
comment on column {{vehdata_schema}}.runs.run is 'Numerical identifier of the lag of a sortie a vehicle is on. May be null, indicating the vehicle is between legs or, if sortie is 0, not in service.';
comment on column {{vehdata_schema}}.runs.time_start is 'Beginning of run (first in a continguous sequence of messages sharing the same combination of values for line_id, sortie and run)';
comment on column {{vehdata_schema}}.runs.time_end is 'End of run (last in a continguous sequence of messages sharing the same combination of values for line_id, sortie and run)';

/* pings table: log of received messages */
create table if not exists {{vehdata_schema}}."pings" (
    vehicle_id integer references {{vehdata_schema}}.vehicles(id),
    time timestamp,
    position geometry(POINT, {{system_srid}}),
    primary key (vehicle_id, time)
);
comment on table {{vehdata_schema}}.pings is 'Position reports received from the vehicles. Note: for this table, all pings are presumed of type `ping`.';
comment on column {{vehdata_schema}}.pings.vehicle_id is 'Identity of the vehicle';
comment on column {{vehdata_schema}}.pings.time is 'Moment in time the message was emitted.';
comment on column {{vehdata_schema}}.pings.time is 'The point position reported by the vehicle.';

/* pings_from_stops: pings carrying information on a stop */
create table if not exists {{vehdata_schema}}."pings_from_stops" (
    kind {{vehdata_schema}}."event_type" check (kind != 'ping'),
    stop_id integer references {{vehdata_schema}}.stops(id),
    expected_time timestamp,
    count_people_boarding integer,
    count_people_disembarking integer,
    primary key (vehicle_id, time)
) inherits ({{vehdata_schema}}."pings");
comment on table {{vehdata_schema}}.pings_from_stops is 'Position reports including information only available if the vehicle stops and opens its doors. This may or happen at the times and places designated by the official schedule, or at any other place and time.';
comment on column {{vehdata_schema}}.pings_from_stops.kind is 'The kind of message sent (see type enum {{vehdata_schema}}.event_type)';
comment on column {{vehdata_schema}}.pings_from_stops.stop_id is 'Identity of the stop. Only set if the vehicle stops at the designated time and location according to the official schedule.';
comment on column {{vehdata_schema}}.pings_from_stops.expected_time is 'Time the vehicle should have reported its position according to the schedule. Note: the reference is the time of departure. Also note: this is not set if the vehicle makes an impromptu stop.';
comment on column {{vehdata_schema}}.pings_from_stops.count_people_boarding is 'Number of people boarding the vehicle, as detected by the onboard counting equipment. Note: only set if the vehicle is equiped and the equipment is operational.';
comment on column {{vehdata_schema}}.pings_from_stops.count_people_disembarking is 'Number of people disembarking the vehicle, as detected by the onboard counting equipment . Note: only set if the vehicle is equiped and the equipment is operational.';


-- method to conveninetly ingest pings as time-series
create function {{vehdata_schema}}.get_pings(veh_id int, time_begin timestamp, time_end timestamp)
returns table(x double precision,
              y double precision,
              "time" timestamp) 
as $$
select 
    st_x(position),
    st_y(position),
    time
from {{vehdata_schema}}.pings
where 
    vehicle_id = veh_id
    and time between time_begin and time_end
    and position is not null
order by time
$$ language SQL;


-- data file management ; May be dropped in the future
create table {{vehdata_schema}}.data_files (
  id uuid NOT NULL primary key,
	filename varchar,
  imported_on timestamp without time zone,
  checksum bytea unique
);
comment on table {{vehdata_schema}}.data_files is 'Data files imported into the system.';
comment on column {{vehdata_schema}}.data_files.id is 'UUID identifying the file locally.';
comment on column {{vehdata_schema}}.data_files.filename is 'Name of the file read in, for convenience (not used for any checks).';
comment on column {{vehdata_schema}}.data_files.imported_on is 'Date and time the import of the file was successfully completed.';
comment on column {{vehdata_schema}}.data_files.checksum is 'SHA256 checksum of the (uncompressed) input data file - used to identify the file.';

create table {{vehdata_schema}}.data_file_timeframes (
  id uuid primary key,
  file_id uuid references {{vehdata_schema}}.data_files(id),
	vehicle_id int references {{vehdata_schema}}.vehicles(id),
  time_start timestamp without time zone,
  time_end timestamp without time zone,
  unique (file_id, vehicle_id)
);
comment on table {{vehdata_schema}}.data_file_timeframes is 'Vehicle timeframes (periods during which vehicles are active, i.e. have runs and pings) occuring in a data file.';
comment on column {{vehdata_schema}}.data_file_timeframes.time_start is 'earliest `time_start` of the `runs` for every `vehicle` occuring in the file.';
comment on column {{vehdata_schema}}.data_file_timeframes.time_end is 'latest `time_end` of the `runs` for every `vehicle` occuring in the file.';
