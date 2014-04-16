use hydradb;
alter table tTemplate add column layout TEXT;
DROP TABLE tDataAttr;
CREATE TABLE tMetadata (
    dataset_id  INT         NOT NULL,
    metadata_name VARCHAR(45) NOT NULL,
    metadata_val  BLOB      NOT NULL,
    PRIMARY KEY (dataset_id, metadata_name),
    FOREIGN KEY (dataset_id) REFERENCES tDataset(dataset_id)
);
