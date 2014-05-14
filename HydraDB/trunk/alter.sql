use hydradb;
alter table tScenario add column locked VARCHAR(1) default 'N' NOT NULL;
alter table tScenario add constraint check (locked in ('Y', 'N'));
