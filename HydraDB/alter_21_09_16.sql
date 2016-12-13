drop index project_name on tProject;
alter table tProject add unique (project_id, created_by, status);
