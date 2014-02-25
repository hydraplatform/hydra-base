/* Permissions*/
insert into tPerm (perm_code, perm_name) values ("add_user",   "Add User");
insert into tPerm (perm_code, perm_name) values ("edit_user",  "Edit User");
insert into tPerm (perm_code, perm_name) values ("add_role",   "Add Role");
insert into tPerm (perm_code, perm_name) values ("edit_role",  "Edit Role");
insert into tPerm (perm_code, perm_name) values ("add_perm",   "Add Permission");
insert into tPerm (perm_code, perm_name) values ("edit_perm",  "Edit Permission");

insert into tPerm (perm_code, perm_name) values ("add_network",    "Add network");
insert into tPerm (perm_code, perm_name) values ("edit_network",   "Edit network");
insert into tPerm (perm_code, perm_name) values ("delete_network", "Delete network");
insert into tPerm (perm_code, perm_name) values ("share_network",  "Share network");
insert into tPerm (perm_code, perm_name) values ("edit_topology",  "Edit network topology");

insert into tPerm (perm_code, perm_name) values ("add_project",    "Add Project");
insert into tPerm (perm_code, perm_name) values ("edit_project",   "Edit Project");
insert into tPerm (perm_code, perm_name) values ("delete_project", "Delete Project");
insert into tPerm (perm_code, perm_name) values ("share_project",  "Share Project");


insert into tPerm (perm_code, perm_name) values ("edit_data", "Edit network data");
insert into tPerm (perm_code, perm_name) values ("view_data", "View network data");

insert into tPerm (perm_code, perm_name) values ("add_template", "Add Template");
insert into tPerm (perm_code, perm_name) values ("edit_template", "Edit Template");

/*insert into tPerm ("", "");*/

/*Roles*/
insert into tRole (role_code, role_name) values ("admin",    "Administrator");
insert into tRole (role_code, role_name) values ("dev",      "Developer");
insert into tRole (role_code, role_name) values ("modeller", "Modeller / Analyst");
insert into tRole (role_code, role_name) values ("manager",  "Manager");
insert into tRole (role_code, role_name) values ("grad",     "Graduate");
insert into tRole (role_code, role_name) values ("decision", "Decision Maker");

DELIMITER // 
CREATE PROCEDURE add_perm_to_role(IN role VARCHAR(45), IN perm VARCHAR(45))
BEGIN
    DECLARE p, r INT default 0;
    select perm_id into p from tPerm where perm_code=perm;
    select role_id into r from tRole where role_code=role;
    insert into tRolePerm (perm_id, role_id) values (p, r);
END;
//

/*Admin user*/
call add_perm_to_role("admin", "add_user");
call add_perm_to_role("admin", "edit_user");
call add_perm_to_role("admin", "add_role");
call add_perm_to_role("admin", "edit_role");
call add_perm_to_role("admin", "add_perm");
call add_perm_to_role("admin", "edit_perm");
call add_perm_to_role("admin", "add_network");
call add_perm_to_role("admin", "edit_network");
call add_perm_to_role("admin", "delete_network");
call add_perm_to_role("admin", "share_network");
call add_perm_to_role("admin", "add_project");
call add_perm_to_role("admin", "edit_project");
call add_perm_to_role("admin", "delete_project");
call add_perm_to_role("admin", "share_project");
call add_perm_to_role("admin", "edit_topology"); 
call add_perm_to_role("admin", "edit_data");
call add_perm_to_role("admin", "view_data");
call add_perm_to_role("admin", "add_template");
call add_perm_to_role("admin", "edit_template");

/*Developer*/
call add_perm_to_role("dev", "add_network");
call add_perm_to_role("dev", "edit_network");
call add_perm_to_role("dev", "delete_network");
call add_perm_to_role("dev", "share_network");
call add_perm_to_role("dev", "add_project");
call add_perm_to_role("dev", "edit_project");
call add_perm_to_role("dev", "delete_project");
call add_perm_to_role("dev", "share_project");
call add_perm_to_role("dev", "edit_topology"); 
call add_perm_to_role("dev", "edit_data");
call add_perm_to_role("dev", "view_data");
call add_perm_to_role("dev", "add_template");
call add_perm_to_role("dev", "edit_template");

/*Modeller*/
call add_perm_to_role("modeller", "add_network");
call add_perm_to_role("modeller", "edit_network");
call add_perm_to_role("modeller", "delete_network");
call add_perm_to_role("modeller", "share_network");
call add_perm_to_role("modeller", "edit_topology"); 
call add_perm_to_role("modeller", "add_project");
call add_perm_to_role("modeller", "edit_project");
call add_perm_to_role("modeller", "delete_project");
call add_perm_to_role("modeller", "share_project");
call add_perm_to_role("modeller", "edit_data");
call add_perm_to_role("modeller", "view_data");

/*Manager*/
call add_perm_to_role("manager", "edit_data");
call add_perm_to_role("manager", "view_data");

/*Graduate*/

/*Decision Maker*/

