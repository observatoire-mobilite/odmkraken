drop database if exists {{dbname}};
create database if exists {{dbname}};

create extension if not exists postgis;

-- create user owning tables
drop user if exists {{roles['admin']['name']}};
create user {{roles['admin']['name']}} with password {{roles['admin']['pw']}} NOSUPERUSER NOCREATEDB NOCREATEROLE;
grant create, connect, temporary on database {{dbname}} to {{roles['admin']['name']}};

-- create read-only user
drop user if exists {{roles['disseminate']['name']}};
create user {{roles['disseminate']['name']}} with password {{roles['disseminate']['pw']}} NOSUPERUSER NOCREATEDB NOCREATEROLE;
grant connect on database {{dbname}} to {{roles['disseminate']['name']}};

-- create user used for processing data
drop user if exists {{roles['process']['name']}};
create user {{roles['process']['name']}} with password {{roles['process']['pw']}} NOSUPERUSER NOCREATEDB NOCREATEROLE;
grant create, connect, temporary on database {{dbname}} to {{roles['process']['name']}};

set role {{roles['admin']['name']}};