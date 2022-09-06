-- create users
create user {{user_owner}};
create user {{user_aoo}};
create user {{user_ro}};

-- give them permissions
revoke all on database {{dbname}} from public;
grant connect, temporary, create on database {{dbname}} to {{user_owner}};
grant connect, temporary on database {{dbname}} to {{user_aoo}};
grant connect on database {{dbname}} to {{user_ro}};
alter database {{dbname}} owner to {{user_owner}};

-- install extensions (usually requires superuser powers)
create extension postgis;

-- the actual table creation, we do as owner, not superuser
set role {{user_owner}};

-- add transport network topology schema
{% include 'db.network.sql' %}

-- add data schema
{% include 'db.vehdata.sql' %}
