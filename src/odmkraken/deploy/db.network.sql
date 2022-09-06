-- create schema storing road network data
create schema {{network_schema}};
comment on schema {{network_schema}} is 'road transport network topology';

-- adjust permissions
grant usage, create on schema {{network_schema}} to {{user_owner}};
grant usage on schema {{network_schema}} to {{user_aoo}};
grant usage on schema {{network_schema}} to {{user_ro}};
alter default privileges in schema {{network_schema}} grant UPDATE, INSERT, SELECT, DELETE on tables to {{user_owner}};
alter default privileges in schema {{network_schema}} grant UPDATE, INSERT, SELECT, DELETE on tables to {{user_aoo}};
alter default privileges in schema {{network_schema}} grant SELECT on tables to {{user_ro}};


-- types describing allowed directions
create type {{network_schema}}."road_edge_allowed_directions"
as ENUM ('both', 'forwards', 'backwards', 'none', 'undetermined');


-- node table
create table {{network_schema}}."road_nodes" (
    id integer primary key,
    geometry Geometry(POINT, {{system_srid}})
);
create index idx_road_nodes_gix on {{network_schema}}."road_nodes" using GIST ("geometry");


-- edge table (links nodes)
create table {{network_schema}}."road_edges" (
    id integer primary key,
    original_tomtom_id bigint,
    from_node integer, --references {{network_schema}}."road_nodes" (id),
    to_node integer, -- references {{network_schema}}."road_nodes" (id),
    allowed_directions {{network_schema}}."road_edge_allowed_directions",
    hierarchy smallint,
    geometry Geometry(LINESTRING, {{system_srid}})
);
create index idx_road_edges_gix on {{network_schema}}."road_edges" using GIST ("geometry");


-- overlay table reflecting exemptions made for busses
create table {{network_schema}}."road_edges_overlay_bus" (
    id integer primary key,
    allowed_directions {{network_schema}}."road_edge_allowed_directions"
);
comment on table {{network_schema}}."road_edges_overlay_bus" is 'This table allows to overwrite the `allowed_directions` in `road_edges` when used through `get_edgelist`';

-- get_edgelist function
create function {{network_schema}}."get_edgelist"() 
returns table(id integer,
              source integer,
              target integer,
              weight double precision,
              forward boolean
             ) 
as $$

with bus_network as (
    select 
        e.id, 
        from_node,
        to_node,
        st_length(geometry) as weight,
        coalesce(o."allowed_directions", e."allowed_directions", 'undetermined'::{{network_schema}}."road_edge_allowed_directions") as allowed_directions
    from {{network_schema}}."road_edges" e
    left join {{network_schema}}."road_edges_overlay_bus" o on o.id = e.id
)

(
    select 
        id, 
        from_node as source, 
        to_node as target,
        weight,
        True as forward
    from bus_network
    where "allowed_directions" in ('forwards', 'both', 'undetermined')
) union (
    select 
        id,
        to_node as source, 
        from_node as target,
        weight,
        False as forward
    from bus_network
    where "allowed_directions" in ('backwards', 'both', 'undetermined')
)
$$ language SQL;
comment on function {{network_schema}}."get_edgelist" is 'get navigation graph for shortest path calculation';

-- nearby_roads function
create function {{network_schema}}."nearby_roads"(double precision, double precision, double precision default 15*6) 
returns table(source int, target int, -- nodes at either end of the segment
              length double precision, -- length of the geometry
              "position" double precision, -- closest relative position on geometry
              heading double precision, -- azimuth of tangent in direction of geometry
              error double precision -- distance from point to geometry
             ) 
as $$

with point as (
    -- convert coordinates to point geometry
    select st_setsrid(st_point($1, $2), {{system_srid}}) as geometry
), candidates as ( 
    -- find geometries within $3 meters around `point.geometry`
    /* *approach*: the `road_edge` table stores road geometries. The geometries are
        LINESTRINGs, which are sequences of points. Thus there is an implied "forward" 
        direction for every road. This first query extracts all road geometries 
        within a distance of $3 of our point ($1, $2), without caring about the
        vehicle's direction of travel. For those metrics, namely `heading` that imply
        a direction of travel, we presume the vehicle is travelling in "forward" direction.
    */
    select  
        e.from_node as source,
        e.to_node as target,
        st_length(e.geometry) as length,
        st_linelocatepoint(e.geometry, p.geometry) as position, -- position from starting point of geometry (regardless of direction of travel)
        -- azimuth at the projected point on the geometry, presuming the vehicle travels in "forwards" direction of the geometry
        -- unless we are at the very end of the linestring, the azimuth is measured between project point on the geometry and a point 0.001 relative length units further "forwards"
        -- if we are at relative position 0.999 or further in forward direction, just use the azimuth between position 0.999 and the geometry's end point
        case when st_linelocatepoint(e.geometry, p.geometry) < 0.999
            then st_azimuth(st_closestpoint(e.geometry, p.geometry), 
                            st_lineinterpolatepoint(e.geometry, st_linelocatepoint(e.geometry, p.geometry) + 0.001))
            else st_azimuth(st_lineinterpolatepoint(e.geometry, 0.999), 
                            st_endpoint(e.geometry))
        end as heading,
        st_distance(e.geometry, p.geometry) as error,
        allowed_directions
    from
        {{network_schema}}."road_edges" e,
        point p
    where
        st_dwithin(e.geometry, p.geometry, $3)
        and (allowed_directions != 'none')  -- discard geometries we can't travel on
        -- only chose geometries if the point ($1, $2) lies to the left or right of the linestring, not "before" or "after"
        and st_linelocatepoint(e.geometry, p.geometry) > 0
        and st_linelocatepoint(e.geometry, p.geometry) < 1
)

/* move from "geometries" to "edges", i.e. describe links in terms of their origin and destination nodes, 
   as a function of the directions the geometry may be legally traversed. */
select 
    source, target, 
    length, position, error, 
    heading 
from candidates
where allowed_directions != 'backwards'
union
select
    target as source, source as target,
    length, 1 - position, error,
    case when heading > pi() then heading - pi() else heading + pi() end as heading
from candidates
where allowed_directions != 'forwards'

$$ language SQL;
comment on function {{network_schema}}."nearby_roads" is 'get roads closest to specified coordinates';