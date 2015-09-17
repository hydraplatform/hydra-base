/* Migrate to newest database version. 

This script makes the following changes to the database without deleting any
data:

- add column 'properties' to ttypeattr.

This code was developed for and tested on a SQLite database. 
 */

/* rename 'network_layout' to 'layout' in tNetwork. */
ALTER TABLE tTypeAttr RENAME TO tmp_tTypeAttr;

CREATE TABLE "tTypeAttr" (
	attr_id INTEGER NOT NULL, 
	type_id INTEGER NOT NULL, 
	default_dataset_id INTEGER, 
	attr_is_var VARCHAR(1) DEFAULT 'N', 
	data_type VARCHAR(60), 
	data_restriction TEXT(1000), 
	unit VARCHAR(60), 
	description TEXT(1000), 
    properties TEXT(1000),
	cr_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, 
	PRIMARY KEY (attr_id, type_id), 
	FOREIGN KEY(attr_id) REFERENCES "tAttr" (attr_id), 
	FOREIGN KEY(type_id) REFERENCES "tTemplateType" (type_id) ON DELETE CASCADE, 
	FOREIGN KEY(default_dataset_id) REFERENCES "tDataset" (dataset_id)
);
INSERT INTO tTypeAttr (attr_id, type_id, default_dataset_id, attr_is_var,
    data_type, data_restriction, unit, description, properties, cr_date) 
select attr_id, type_id, default_dataset_id, attr_is_var,
    data_type, data_restriction, unit, description, NULL, cr_date
    FROM tmp_tTypeAttr;
DROP TABLE tmp_tTypeAttr;
