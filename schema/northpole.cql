drop keyspace northpole;

create keyspace northpole WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

use northpole;

create table children ( child_id varchar, first_name varchar, last_name varchar, country varchar, state varchar, zip varchar, primary key (child_id ) ) WITH COMPACT STORAGE;
create table naughtyOrNiceList ( standingByCountry varchar, state varchar, zip varchar, child_id varchar, primary key (standingByCountry, state, zip, child_id) );

insert into children (child_id, first_name, last_name, country, state, zip) values ('owen.oneill', 'Owen', 'O''Neill', 'IRL', 'D', 'EI33');
insert into children (child_id, first_name, last_name, country, state, zip) values ('collin.oneill', 'Collin', 'O''Neill', 'IRL', 'D', 'EI33');
insert into children (child_id, first_name, last_name, country, state, zip) values ('richie.rich', 'Richie', 'Rich', 'USA', 'CA', '94333');
insert into children (child_id, first_name, last_name, country, state, zip) values ('johny.b.good', 'Johny', 'Good', 'USA', 'CA', '94333');
insert into children (child_id, first_name, last_name, country, state, zip) values ('bart.simpson', 'Bart', 'Simpson', 'USA', 'CA', '94111');
insert into children (child_id, first_name, last_name, country, state, zip) values ('dennis.menace', 'Dennis', 'Menace', 'USA', 'CA', '94222');
insert into children (child_id, first_name, last_name, country, state, zip) values ('michael.myers', 'Michael', 'Myers', 'USA', 'PA', '18964');

insert into naughtyOrNiceList (standingByCountry, state, zip, child_id) values ('nice:IRL', 'D', 'EI33','owen.oneill');
insert into naughtyOrNiceList (standingByCountry, state, zip, child_id) values ('nice:IRL', 'D', 'EI33', 'collin.oneill');
insert into naughtyOrNiceList (standingByCountry, state, zip, child_id) values ('nice:USA', 'CA', '94333', 'richie.rich');
insert into naughtyOrNiceList (standingByCountry, state, zip, child_id) values ('nice:USA', 'CA', '94333', 'johny.b.good');
insert into naughtyOrNiceList (standingByCountry, state, zip, child_id) values ('naughty:USA', 'CA', '94111', 'bart.simpson');
insert into naughtyOrNiceList (standingByCountry, state, zip, child_id) values ('naughty:USA', 'CA', '94222', 'dennis.menace');
insert into naughtyOrNiceList (standingByCountry, state, zip, child_id) values ('naughty:USA', 'PA', '18964', 'michael.myers');

