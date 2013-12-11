DROP DATABASE IF EXISTS hydradb;
CREATE DATABASE hydradb;

USE hydradb;

/* Project network and scenearios */

CREATE TABLE tProject (
    project_id          INT           NOT NULL PRIMARY KEY AUTO_INCREMENT,
    project_name        VARCHAR(45)   NOT NULL,
    project_description VARCHAR(1000),
    status              VARCHAR(1) default 'A' NOT NULL,
    cr_date  TIMESTAMP default localtimestamp,
    created_by          INT,
    UNIQUE (project_name)
);

insert into tProject (project_name) values ('Default Project');

CREATE TABLE tNetwork (
    network_id          INT          NOT NULL PRIMARY KEY AUTO_INCREMENT,
    network_name        VARCHAR(45)  NOT NULL,
    network_description VARCHAR(1000),
    network_layout      VARCHAR(1000),
    project_id          INT          NOT NULL,
    status              VARCHAR(1) default 'A' NOT NULL,
    cr_date  TIMESTAMP default localtimestamp,
    projection          VARCHAR(1000),
    FOREIGN KEY (project_id) REFERENCES tProject(project_id),
    UNIQUE (project_id, network_name)
);

insert into tNetwork(project_id, network_name) values (1, 'Default Network');

CREATE TABLE tNode (
    node_id          INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
    network_id       INT         NOT NULL,
    node_description VARCHAR(45),
    node_name        VARCHAR(45) NOT NULL,
    status           VARCHAR(1) default 'A' NOT NULL,
    node_x           DOUBLE,
    node_y           DOUBLE,
    node_layout      VARCHAR(1000),
    cr_date  TIMESTAMP default localtimestamp,
    FOREIGN KEY (network_id) REFERENCES tNetwork(network_id),
    UNIQUE (network_id, node_name)
);

CREATE TABLE tLink (
    link_id         INT          NOT NULL PRIMARY KEY AUTO_INCREMENT,
    network_id      INT          NOT NULL,
    status          VARCHAR(1) default 'A' NOT NULL,
    node_1_id       INT          NOT NULL,
    node_2_id       INT          NOT NULL,
    link_name       VARCHAR(45),
    link_layout     VARCHAR(1000),
    cr_date  TIMESTAMP default localtimestamp,
    FOREIGN KEY (network_id) REFERENCES tNetwork(network_id),
    FOREIGN KEY (node_1_id) REFERENCES tNode(node_id),
    FOREIGN KEY (node_2_id) REFERENCES tNode(node_id),
    UNIQUE (node_1_id, node_2_id, link_name)
);

CREATE TABLE tScenario (
    scenario_id          INT           NOT NULL PRIMARY KEY AUTO_INCREMENT,
    scenario_name        VARCHAR(45)   NOT NULL,
    scenario_description VARCHAR(1000),
    status               VARCHAR(1) default 'A' NOT NULL,
    network_id           INT,
    cr_date  TIMESTAMP default localtimestamp,
    FOREIGN KEY (network_id) REFERENCES tNetwork(network_id),
    UNIQUE (network_id, scenario_name)
);

/*"Project scenario" (scenario_id 1) is the container for all project-related attributes.
  As data must be contained in a scenario, but projects do not have scenarios.
  This one scenario is a special case, only for project attributes.
*/
insert into tScenario (network_id, scenario_name) values (1, 'Project Scenario');

CREATE TABLE tDataset (
    dataset_id  INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
    data_id     INT         NOT NULL,
    data_type   VARCHAR(45) NOT NULL,
    data_units  VARCHAR(45),
    data_dimen  VARCHAR(45),
    data_name   VARCHAR(45) NOT NULL,
    data_hash   BIGINT      NOT NULL,
    cr_date  TIMESTAMP default localtimestamp,
    constraint chk_type check (data_type in ('descriptor', 'timeseries',
    'eqtimeseries', 'scalar', 'array'))
);

/* Attributes */

CREATE TABLE tAttr (
    attr_id    INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
    attr_name  VARCHAR(45) NOT NULL,
    attr_dimen VARCHAR(45),
    default_dataset_id  INT,
    attr_is_var VARCHAR(1),
    cr_date  TIMESTAMP default localtimestamp,
    UNIQUE (attr_name, attr_dimen),
    FOREIGN KEY (default_dataset_id) REFERENCES tDataset(dataset_id),
    check (is_var in ('Y', 'N'))
);

CREATE TABLE tResourceTemplateGroup (
    group_id   INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
    group_name VARCHAR(45) NOT NULL,
    UNIQUE(group_name)
);

insert into tResourceTemplateGroup (group_name) values ('Default');

CREATE TABLE tResourceTemplate(
    template_id   INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
    template_name VARCHAR(45) NOT NULL,
    group_id INT,
    FOREIGN KEY (group_id) REFERENCES tResourceTemplateGroup(group_id),
    UNIQUE(group_id, template_name)
);

CREATE TABLE tResourceTemplateItem (
    attr_id     INT NOT NULL,
    template_id INT NOT NULL,
    PRIMARY KEY (attr_id, template_id),
    FOREIGN KEY (attr_id) REFERENCES tAttr(attr_id),
    FOREIGN KEY (template_id) REFERENCES tResourceTemplate(template_id)
);

CREATE TABLE tAttrMap (
    attr_id_a INT NOT NULL,
    attr_id_b INT NOT NULL,
    PRIMARY KEY (attr_id_a, attr_id_b),
    FOREIGN KEY (attr_id_a) REFERENCES tAttr(attr_id),
    FOREIGN KEY (attr_id_b) REFERENCES tAttr(attr_id)
);

CREATE TABLE tResourceType (
    ref_key          VARCHAR(45) NOT NULL,
    ref_id           INT         NOT NULL,
    template_id      int         NOT NULL,
    FOREIGN KEY (template_id) REFERENCES tResourceTemplate(template_id),
    PRIMARY KEY (ref_key, ref_id, template_id),
    CHECK (ref_key in ('NODE', 'LINK', 'NETWORK'))
);

CREATE TABLE tResourceAttr (
    resource_attr_id INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
    attr_id          INT         NOT NULL,
    ref_key          VARCHAR(45) NOT NULL,
    ref_id           INT         NOT NULL,
    attr_is_var      VARCHAR(1)  NOT NULL default 'N',
    FOREIGN KEY (attr_id) REFERENCES tAttr(attr_id)
);
CREATE UNIQUE INDEX iResourceAttr_1 ON tResourceAttr (attr_id, ref_key, ref_id);
CREATE INDEX iResourceAttr_2 ON tResourceAttr (ref_key, ref_id);

/* Constraints */

CREATE TABLE tConstraint (
    constraint_id INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
    scenario_id   INT         NOT NULL,
    group_id      INT,
    constant      DOUBLE      NOT NULL,
    op            VARCHAR(10) NOT NULL,
    FOREIGN KEY (scenario_id) REFERENCES tScenario(scenario_id)
);

CREATE TABLE tConstraintItem (
    item_id          INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    constraint_id    INT NOT NULL,
    resource_attr_id INT,
    constant         BLOB,
    CHECK ((constant is null and resource_attr_id is not null) or (constant is not null and resource_attr_id is null)), 
    FOREIGN KEY (constraint_id) REFERENCES tConstraint(constraint_id),
    FOREIGN KEY (resource_attr_id) REFERENCES tResourceAttr(resource_Attr_id)
);

CREATE TABLE tConstraintGroup (
    group_id      INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
    constraint_id INT         NOT NULL,
    ref_key_1     VARCHAR(45) NOT NULL,
    ref_id_1      INT         NOT NULL,
    ref_key_2     VARCHAR(45),
    ref_id_2      INT,
    op            VARCHAR(10),
    CHECK (ref_key_1 in ('GRP', 'ITEM')),
    CHECK (ref_key_2 is null or ref_key_2 in ('GRP', 'ITEM')),
    FOREIGN KEY (constraint_id) REFERENCES tConstraint(constraint_id)
);

/* Data representation */

CREATE TABLE tDescriptor (
    data_id  INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
    desc_val VARCHAR(45) NOT NULL
);

CREATE TABLE tTimeSeries (
    data_id  INT      NOT NULL PRIMARY KEY AUTO_INCREMENT
);

CREATE TABLE tTimeSeriesData(
    data_id  INT         NOT NULL,
    ts_time  DOUBLE PRECISION(20, 10) UNSIGNED NOT NULL,
    ts_value BLOB        NOT NULL,
    PRIMARY KEY (data_id, ts_time),
    FOREIGN KEY (data_id) references tTimeSeries(data_id)
);

CREATE TABLE tEqTimeSeries (
    data_id       INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
    start_time    DOUBLE PRECISION(20, 10) UNSIGNED NOT NULL,
    frequency     DOUBLE      NOT NULL,
    arr_data      BLOB        NOT NULL
);

CREATE TABLE tScalar (
    data_id     INT    NOT NULL PRIMARY KEY AUTO_INCREMENT,
    param_value DOUBLE NOT NULL
);

CREATE TABLE tArray (
    data_id        INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    arr_data       BLOB        NOT NULL
);

CREATE TABLE tDatasetGroup (
    group_id    INT     NOT NULL PRIMARY KEY AUTO_INCREMENT,
    group_name  VARCHAR(45) NOT NULL,
    cr_date     TIMESTAMP default localtimestamp,
    UNIQUE (group_name)
);

CREATE TABLE tDatasetGroupItem (
    group_id    INT    NOT NULL,
    dataset_id  INT    NOT NULL,
    PRIMARY KEY (group_id, dataset_id),
    FOREIGN KEY (group_id) REFERENCES tDatasetGroup(group_id),
    FOREIGN KEY (dataset_id) REFERENCES tDataset(dataset_id)
);

CREATE TABLE tDataAttr (
    d_attr_id   INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
    dataset_id  INT         NOT NULL,
    d_attr_name VARCHAR(45) NOT NULL,
    d_attr_val  DOUBLE      NOT NULL,
    FOREIGN KEY (dataset_id) REFERENCES tDataset(dataset_id)
);

CREATE TABLE tResourceScenario (
    dataset_id          INT NOT NULL,
    scenario_id      INT NOT NULL,
    resource_attr_id INT NOT NULL,
    PRIMARY KEY (resource_attr_id, scenario_id),
    FOREIGN KEY (scenario_id) REFERENCES tScenario(scenario_id),
    FOREIGN KEY (dataset_id) REFERENCES tDataset(dataset_id),
    FOREIGN KEY (resource_attr_id) REFERENCES tResourceAttr(resource_attr_id)
);

/* ========================================================================= */
/* User permission management                                                */

CREATE TABLE tUser (
    user_id  INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
    username varchar(45) NOT NULL,
    password varchar(1000) NOT NULL,
    last_login DATETIME,
    last_edit  DATETIME,
    cr_date  TIMESTAMP default localtimestamp
);

CREATE TABLE tRole (
    role_id   INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
    role_name VARCHAR(45) NOT NULL,
    cr_date  TIMESTAMP default localtimestamp
);

CREATE TABLE tPerm (
    perm_id   INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
    perm_name VARCHAR(45) NOT NULL,
    cr_date  TIMESTAMP default localtimestamp
);

CREATE TABLE tRoleUser (
    user_id INT NOT NULL,
    role_id INT NOT NULL,
    cr_date  TIMESTAMP default localtimestamp,
    PRIMARY KEY (user_id, role_id),
    FOREIGN KEY (user_id) REFERENCES tUser(user_id),
    FOREIGN KEY (role_id) REFERENCES tRole(role_id)
);

CREATE TABLE tRolePerm (
    perm_id INT NOT NULL,
    role_id INT NOT NULL,
    cr_date  TIMESTAMP default localtimestamp,
    PRIMARY KEY (perm_id, role_id),
    FOREIGN KEY (perm_id) REFERENCES tPerm(perm_id),
    FOREIGN KEY (role_id) REFERENCES tRole(role_id)
);

CREATE TABLE tProjectOwner (
    user_id INT NOT NULL,
    project_id INT NOT NULL,
    cr_date  TIMESTAMP default localtimestamp,
    PRIMARY KEY (user_id, project_id),
    FOREIGN KEY (user_id) REFERENCES tUser(user_id),
    FOREIGN KEY (project_id) REFERENCES tProject(project_id)
);

CREATE TABLE tDatasetOwner (
    user_id INT NOT NULL,
    dataset_id INT NOT NULL,
    cr_date  TIMESTAMP default localtimestamp,
    PRIMARY KEY (user_id, dataset_id),
    FOREIGN KEY (user_id) REFERENCES tUser(user_id),
    FOREIGN KEY (dataset_id) REFERENCES tDataset(dataset_id)
);
