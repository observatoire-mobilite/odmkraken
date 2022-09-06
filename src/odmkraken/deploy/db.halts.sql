/* halting at bus stops */
create function {{vehdata_schema}}.get_vehicle_halts(veh_id int, time_begin timestamp, time_end timestamp)
returns table(vehicle_id int,
              "time" timestamp,
              "position" geometry,
              stop_id int,
              scheduled_time timestamp,
	          dt_stationary interval,
              count_people_boarding int,
              count_people_disembarking int) 
as $$
select 
	vehicle_id, time, position, stop_id, expected_time as scheduled_time,
	dt_stationary, count_people_boarding, count_people_disembarking
from (
	select 
		vehicle_id, time, position, kind, stop_id, expected_time, count_people_boarding, count_people_disembarking,
		time - lag(time) over (partition by vehicle_id order by vehicle_id, time) as dt_stationary
	from {{vehdata_schema}}.pings as p
	natural inner join {{vehdata_schema}}.pings_from_stops as s
    where vehicle_id = veh_id and time between time_begin and time_end
) as foo where kind is not null and kind != 'Störungspunkt'
$$ language SQL;
comment on function {{vehdata_schema}}.get_vehicle_halts is 'Retrieves information about the halts that vehicle `vehicle_id` makes at known (or unknown) stops of the public transport network within the period between `time_begin` and `time_end`';


create table {{vehdata_schema}}.halts (
    vehicle_id int references {{vehdata_schema}}.vehicles(id),
    "time" timestamp,
    "position" geometry(POINT, {{system_srid}}),
    stop_id int references {{vehdata_schema}}.stops(id),
    scheduled_time timestamp,
    dt_stationary interval,
    count_people_boarding int,
    count_people_disembarking int,
    primary key (vehicle_id, "time")
);
comment on table {{vehdata_schema}}.halts is 'A halt is defined as a vehicle stopping and opening its doors to let passengers in or out. Each row in this tables corresponds to the vehicle closing its doors and driving away again after halting (obviously the passenger count is only defined after everybody got in or out).';
comment on column {{vehdata_schema}}.halts.vehicle_id is 'Vehicle that reported the halt.';
comment on column {{vehdata_schema}}.halts.time is 'Time at which the vehicle reported its halt. This is always the end of the halt, i.e. as the vehicle closes its doors and drives away.';
comment on column {{vehdata_schema}}.halts.position is 'Position as reported by the vehicle''s GNSS receiver. Theoretically, the field should always be set. Practically, < 1% of pings (of any kind) have no spatial information. Mapmatching ignores them, but those sent during halts will pop up here (meaning this field may be null). Note that the reported position may deviate from the official coordinates of the stop (if reported).';
comment on column {{vehdata_schema}}.halts.stop_id is 'The stop reported by the vehicle. If not set, the halt occured outside the official stop network.';
comment on column {{vehdata_schema}}.halts.scheduled_time is 'Indicates the time at which a vehicle was supposed to halt according to its schedule. If not set, the halt was unscheduled.';
comment on column {{vehdata_schema}}.halts.dt_stationary is 'Time the vehicle spent standing still. Note that since `time` refers to the end of a halt, `dt_stationary` measures the time backwards to the moment the vehicle physically stopped moving.';
comment on column {{vehdata_schema}}.halts.count_people_boarding is 'Number of people that boarded the bus while it stood still. If not set, the vehicle had no functional counting equipment during its halt. Beware: counters may miss passengers, especially at the terminus (while the engine is shut off).';
comment on column {{vehdata_schema}}.halts.count_people_disembarking is 'Number of people that left the bus while it stood still. If not set, the vehicle had no functional counting equipment during its halt. Beware: counters may miss passengers, especially at the terminus (while the engine is shut off).';
