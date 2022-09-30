-- create vehicle data schema and set permissions
create schema {{vehdata_schema}};
comment on schema {{vehdata_schema}} is 'raw data from the init tracking system for vehicles of public transport';

grant usage, create on schema {{vehdata_schema}} to {{user_owner}};
grant usage, create on schema {{vehdata_schema}} to {{user_aoo}};  -- needs create for sequences and temp tables; should it though ??
grant usage on schema {{vehdata_schema}} to {{user_ro}};

alter default privileges in schema {{vehdata_schema}} grant ALL on tables to {{user_owner}};
alter default privileges in schema {{vehdata_schema}} grant UPDATE, INSERT, SELECT, DELETE on tables to {{user_aoo}};
alter default privileges in schema {{vehdata_schema}} grant USAGE, SELECT, UPDATE on sequences to {{user_aoo}};
alter default privileges in schema {{vehdata_schema}} grant SELECT on tables to {{user_ro}};


-- tables for raw data storage
{% include 'db.rawdata.sql' %}

-- tables for resuls storage
{% include 'db.results.sql' %}
{% include 'db.halts.sql' %}