create keyspace if not exists ks with replication = {'class':'SimpleStrategy', 'replication_factor':1};

create table if not exists ks.people (
  id int primary key,
  name text,
  birth_date timestamp
);

create index on ks.people (name);

insert into ks.people (id, name, birth_date) values (10, 'Catherine', '1987-12-02');
insert into ks.people (id, name, birth_date) values (11, 'Isadora', '2004-09-08');
insert into ks.people (id, name, birth_date) values (12, 'Anna', '1970-10-02');
