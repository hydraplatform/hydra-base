/* Migrate to newest database version. 

This script makes the following changes to the database without deleting any
data:

- column names 'network_layout', 'scenario_layout', 'node_layout' and 
  'link_layout' are changed to 'layout'.

- A column 'attr_description' is added to tAttr.

This code was developed for and tested on a SQLite database. 
 */

/* rename 'network_layout' to 'layout' in tNetwork. */
ALTER TABLE tNetwork RENAME TO tmp_tNetwork;
CREATE TABLE "tNetwork" (
	network_id INTEGER NOT NULL, 
	network_name VARCHAR(60) NOT NULL, 
	network_description VARCHAR(1000), 
	layout TEXT(1000), 
	project_id INTEGER NOT NULL, 
	status VARCHAR(1) DEFAULT 'A' NOT NULL, 
	cr_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, 
	projection VARCHAR(1000), 
	created_by INTEGER, 
	PRIMARY KEY (network_id), 
	CONSTRAINT "unique net name" UNIQUE (network_name, project_id), 
	FOREIGN KEY(project_id) REFERENCES "tProject" (project_id), 
	FOREIGN KEY(created_by) REFERENCES "tUser" (user_id)
);
INSERT INTO tNetwork (network_id, network_name, network_description, layout,
    project_id, status, cr_date, projection, created_by) 
SELECT network_id, network_name, network_description, network_layout,
    project_id, status, cr_date, projection, created_by
    FROM tmp_tNetwork;
DROP TABLE tmp_tNetwork;

/* rename 'scenario_layout' to 'layout' in tScenario. */
ALTER TABLE tScenario RENAME TO tmp_tScenario;
CREATE TABLE "tScenario" (
	scenario_id INTEGER NOT NULL, 
	scenario_name VARCHAR(60) NOT NULL, 
	scenario_description VARCHAR(1000), 
	layout TEXT(1000), 
	status VARCHAR(1) DEFAULT 'A' NOT NULL, 
	network_id INTEGER, 
	start_time VARCHAR(60), 
	end_time VARCHAR(60), 
	locked VARCHAR(1) DEFAULT 'N' NOT NULL, 
	time_step VARCHAR(60), 
	cr_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, 
	created_by INTEGER, 
	PRIMARY KEY (scenario_id), 
	CONSTRAINT "unique scenario name" UNIQUE (network_id, scenario_name), 
	FOREIGN KEY(network_id) REFERENCES "tNetwork" (network_id), 
	FOREIGN KEY(created_by) REFERENCES "tUser" (user_id)
);
INSERT INTO tScenario (scenario_id, scenario_name, scenario_description, layout,
	status, network_id, start_time, end_time, locked, time_step, cr_date,
	created_by)
SELECT scenario_id, scenario_name, scenario_description, scenario_layout,
    status, network_id, start_time, end_time, locked, time_step, cr_date,
    created_by FROM tmp_tScenario;
DROP TABLE tmp_tScenario;

/* rename 'node_layout' to 'layout' in tNode. */
ALTER TABLE tNode RENAME TO tmp_tNode;
CREATE TABLE "tNode" (
	node_id INTEGER NOT NULL, 
	network_id INTEGER NOT NULL, 
	node_description VARCHAR(1000), 
	node_name VARCHAR(60) NOT NULL, 
	status VARCHAR(1) DEFAULT 'A' NOT NULL, 
	node_x FLOAT, 
	node_y FLOAT, 
	layout TEXT(1000), 
	cr_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, 
	PRIMARY KEY (node_id), 
	CONSTRAINT "unique node name" UNIQUE (network_id, node_name), 
	FOREIGN KEY(network_id) REFERENCES "tNetwork" (network_id)
);
INSERT INTO tNode (node_id, network_id, node_description, node_name, status,
	node_x, node_y, layout, cr_date)
SELECT node_id, network_id, node_description, node_name, status,
	node_x, node_y, node_layout, cr_date FROM tmp_tNode;
DROP TABLE tmp_tNode;

/* rename 'link_layout' to 'layout' in tLink. */
ALTER TABLE tLink RENAME TO tmp_tLink;
CREATE TABLE "tLink" (
	link_id INTEGER NOT NULL, 
	network_id INTEGER NOT NULL, 
	status VARCHAR(1) DEFAULT 'A' NOT NULL, 
	node_1_id INTEGER NOT NULL, 
	node_2_id INTEGER NOT NULL, 
	link_name VARCHAR(60), 
	link_description VARCHAR(1000), 
	layout TEXT(1000), 
	cr_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, 
	PRIMARY KEY (link_id), 
	CONSTRAINT "unique link name" UNIQUE (network_id, link_name), 
	FOREIGN KEY(network_id) REFERENCES "tNetwork" (network_id), 
	FOREIGN KEY(node_1_id) REFERENCES "tNode" (node_id), 
	FOREIGN KEY(node_2_id) REFERENCES "tNode" (node_id)
);
INSERT INTO tLink (link_id, network_id, status, node_1_id, node_2_id, link_name,
	link_description, layout, cr_date)
SELECT link_id, network_id, status, node_1_id, node_2_id, link_name,
	link_description, link_layout, cr_date FROM tmp_tLink;
DROP TABLE tmp_tLink;

/* add column 'attr_description' to tAttr. */
ALTER TABLE tAttr RENAME TO tmp_tAttr;
CREATE TABLE tAttr (
	attr_id INTEGER NOT NULL, 
	attr_name VARCHAR(60) NOT NULL, 
	attr_dimen VARCHAR(60), 
    attr_description VARCHAR(1000),
	cr_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, 
	PRIMARY KEY (attr_id), 
	CONSTRAINT "unique name dimension" UNIQUE (attr_name, attr_dimen)
);
INSERT INTO tAttr (attr_id, attr_name, attr_dimen, cr_date) 
SELECT attr_id, attr_name, attr_dimen, cr_date FROM tmp_tAttr;
DROP TABLE tmp_tAttr;
