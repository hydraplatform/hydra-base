use hydradb;
alter table tTypeAttr change attr_is_var attr_is_var VARCHAR(1) default 'N';
alter table tTypeAttr add column dimension   VARCHAR(45);
