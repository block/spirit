use mysql;

create role if not exists R_MIGRATOR;
grant alter, create, create temporary tables, delete, drop, index, insert, lock tables, select, trigger, update, reload on *.* to R_MIGRATOR;
create role if not exists R_REPLICATION;
grant replication slave, replication client on *.* to R_REPLICATION;
create role if not exists R_THROTTLER;
grant select on performance_schema.replication_applier_status_by_worker to R_THROTTLER;
grant select on performance_schema.replication_connection_status to R_THROTTLER;
create role if not exists R_FORCEKILL;
grant connection_admin, process on *.* to R_FORCEKILL;
grant select on performance_schema.* to R_FORCEKILL;

create user if not exists msandbox@'%' identified with caching_sha2_password by 'msandbox';
grant R_MIGRATOR, R_REPLICATION, R_FORCEKILL to msandbox@'%';
set default role R_MIGRATOR, R_REPLICATION, R_FORCEKILL to msandbox@'%';

create user if not exists rsandbox@'%' identified with caching_sha2_password by 'rsandbox';
grant R_REPLICATION, R_THROTTLER to rsandbox@'%';
set default role R_REPLICATION, R_THROTTLER to rsandbox@'%';

-- unfortunately the password for tsandbox must be the same as the password for root because in tests we switch to root
-- using the same password.
create user if not exists tsandbox@'%' identified with caching_sha2_password by 'msandbox';
grant R_MIGRATOR, R_REPLICATION, R_FORCEKILL to tsandbox@'%';
grant event on *.* to tsandbox@'%'; -- information_schema.EVENTS lists only events the user has EVENT on: sync's target check reads it, and move's privileges check requires EVENT (routines are visible through R_MIGRATOR's global SELECT)
grant references on *.* to tsandbox@'%'; -- used in tests
grant system_variables_admin on *.* to tsandbox@'%'; -- replaces SUPER, available since MySQL 8.0
set default role R_MIGRATOR, R_REPLICATION, R_FORCEKILL to tsandbox@'%';

flush privileges;

-- Allow non-SUPER users to create triggers/functions with binary logging enabled.
-- This replaces the SUPER privilege which was removed in MySQL 9.0.
set persist log_bin_trust_function_creators = 1;

create database if not exists test;

use test;

create table t1
(
    id int not null primary key auto_increment,
    b  int not null,
    c  int not null
);
