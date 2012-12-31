-- group example

create table employee (
  name varchar(20) not null, 
  age smallint not null,
  salary double,
  department varchar(20),
  full_time bool);

insert employee values
('John', '20', '1800', 'Accounting', 'false'),
('Darrel', '25', '3300', 'Sales', 'true'),
('Greg', '32', '4800', 'Sales', 'false'),
('Amanda', '31', 3500, 'IT', 'true'),
('Stacy', '33', '1900', 'IT', 'false');


-- join example

create table author (
  author_id int not null,
  name varchar(20) not null,
  nobel bool not null
);

create table book (
  book_id int not null,
  author_id_ref int references author(author_id),
  title varchar(20) not null,
  date_published date
);

alter table author modify author_id int not null auto_increment, add primary (author_id);

insert author (name, nobel) values
  ('Terry Pratchett', 0),
  ('Chuck Palahniuk', 0),
  ('Ernest Hemingway',1);

truncate table author;

alter table book modify book_id int not null auto_increment, add primary key (book_id);

insert into author values (NULL, 'Terry Pratchett', 0);
select @last := LAST_INSERT_ID();
insert into book values
  (NULL, @last, "The Reaper Man", "1991/01/01"),
  (NULL, @last, "Colour of Magic", "1983/01/01"),
  (NULL, @last, "Light Fantastic", "1986/01/01"),
  (NULL, @last, "Mort", NULL);

insert into author values (NULL, 'Chuck Palahniuk', 0);
select @last := LAST_INSERT_ID();
insert into book values
  (NULL, @last, "Fight Club", "1996/01/01"),
  (NULL, @last, "Survivor", NULL),
  (NULL, @last, "Choke", "2001/01/01");

insert into author values (NULL, 'Ernest Hemingway',1);
select @last := LAST_INSERT_ID();
insert into book values
  (NULL, @last, "The old man and the sea", NULL),
  (NULL, @last, "For whom the bell tolls", NULL),
  (NULL, @last, "A farewell to arms", "1929/01/01");

insert into book values
  (NULL, NULL, "Carpet People", NULL),
  (NULL, NULL, "Producing open source software.", NULL),
  (NULL, NULL, "Quantum computation and quantum information.", NULL);



