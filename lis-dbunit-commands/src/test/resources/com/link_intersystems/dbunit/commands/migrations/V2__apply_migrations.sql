ALTER TABLE actor RENAME COLUMN first_name TO firstname;
ALTER TABLE actor RENAME COLUMN last_name TO lastname;

create table film_description (film_id smallint not null, description clob, primary key (film_id));

insert into film_description(film_id, description)
        select film_id, description from film;

alter table film drop column description;