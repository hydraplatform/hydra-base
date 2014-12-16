# (c) Copyright 2013, 2014, University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
from sqlalchemy import create_engine,\
        MetaData,\
        Table,\
        Column,\
        Integer,\
        String,\
        TIMESTAMP,\
        text,\
        DDL
from sqlalchemy.engine import reflection
import logging
from mysql.connector.connection import MySQLConnection
from HydraLib import config
from subprocess import Popen
from sqlalchemy.types import DECIMAL, NUMERIC
from sqlalchemy.dialects.mysql.base import DOUBLE
from decimal import Decimal
import os

engine_name = config.get('mysqld', 'url')
sqlite_engine = "sqlite:///%s"%(config.get('sqlite', 'backup_url'))

def connect():
    """
        return an inspector object
    """
   # MySQLConnection.get_characterset_info = MySQLConnection.get_charset

    db = create_engine(engine_name, echo=True)
    db.connect()
    
    return db

def create_sqlite_backup_db(audit_tables):
    """
        return an inspector object
    """
    #we always want to create a whole new DB, so delete the old one first
    #if it exists.
    try:
        Popen("rm %s"%(config.get('sqlite', 'backup_url')), shell=True)
        logging.warn("Old sqlite backup DB removed")
    except Exception, e:
        logging.warn(e)

    try:
        aux_dir = config.get('DEFAULT', 'hydra_aux_dir')
        os.mkdir(aux_dir)
        logging.warn("%s created", aux_dir)
    except Exception, e:
        logging.warn(e)

    try:
        backup_dir = config.get('db', 'export_target')
        os.mkdir(backup_dir)
        logging.warn("%s created", backup_dir)
    except Exception, e:
        logging.warn(e)

    db = create_engine(sqlite_engine, echo=True)
    db.connect()
    metadata = MetaData(db)
   
    for main_audit_table in audit_tables:
        cols = []
        for c in main_audit_table.columns:
            col = c.copy()
            if col.type.python_type == Decimal:
                col.type = DECIMAL()

            cols.append(col)
        Table(main_audit_table.name, metadata, *cols, sqlite_autoincrement=True)

    metadata.create_all(db)

def run():
    db = create_engine(engine_name, echo=True)
    db = connect()
    metadata = MetaData(db)
    insp = reflection.Inspector.from_engine(db)
    tables = []
    for table_name in insp.get_table_names():
        table = Table(table_name, metadata, autoload=True, autoload_with=db)
        if not table_name.endswith('_aud'):
            tables.append(table)
        else:
            table.drop(db)
            metadata.remove(table)        

    audit_tables = []
    for t in tables:
        audit_table = create_audit_table(t)
        audit_tables.append(audit_table)
        
    create_sqlite_backup_db(audit_tables)
    create_triggers(db, tables)
    metadata.create_all()

def create_triggers(db, tables):


    db = create_engine(engine_name)
    db.echo = True
    db.connect()
    metadata = MetaData(db)


    insp = reflection.Inspector.from_engine(db)

    tables = []
    for table_name in insp.get_table_names():
        if not table_name.endswith('_aud'):
            table = Table(table_name, metadata, autoload=True, autoload_with=db)
            tables.append(table)
            #print("TABLE: %s"%table)
            #print table.__repr__
        else:
            table = Table(table_name, metadata, autoload=True, autoload_with=db)
            table.drop(db)
            metadata.remove(table)        


    drop_trigger_text = """DROP TRIGGER IF EXISTS %(trigger_name)s;"""
    for table in tables:
        pk_cols = [c.name for c in table.primary_key]
        for pk_col in pk_cols:
            try:
                db.execute(drop_trigger_text % {
                    'trigger_name' : table.name + "_ins_trig",
                })
            except:
                pass

        for pk_col in pk_cols:
            try:
                db.execute(drop_trigger_text % {
                    'trigger_name' : table.name + "_upd_trig",
                })
            except:
                pass
    #metadata.create_all()

    trigger_text = """
                    CREATE TRIGGER
                        %(trigger_name)s
                    AFTER %(action)s ON
                        %(table_name)s
                    FOR EACH ROW
                        BEGIN
                            INSERT INTO %(table_name)s_aud
                            SELECT
                                d.*,
                                '%(action)s',
                                NULL,
                                date('now')
                            FROM
                                %(table_name)s
                                AS d
                            WHERE
                                %(pkd)s;
                        END
                        """
    
    for table in tables:


        pk_cols = [c.name for c in table.primary_key]
        pkd = []
        
        for pk_col in pk_cols:
            pkd.append("d.%s = NEW.%s"%(pk_col, pk_col))

        text_dict = {
            'action'       : 'INSERT',
            'trigger_name' : table.name + "_ins_trig",
            'table_name'   : table.name,
            'pkd'           : ' and '.join(pkd),
        }

        logging.info(trigger_text % text_dict)
        trig_ddl = DDL(trigger_text % text_dict)
        trig_ddl.execute_at('after-create', table.metadata)  

        text_dict['action'] = 'UPDATE'
        text_dict['trigger_name'] = table.name + "_upd_trig"
        trig_ddl = DDL(trigger_text % text_dict)
        trig_ddl.execute_at('after-create', table.metadata)  

    metadata.create_all()

def create_audit_table(table):
    args = []
    for c in table.columns:
        col = c.copy()
        #if col.name == 'cr_date':
        #    continue
        col.primary_key=False
        col.foreign_keys = set([])
        col.server_default=None
        col.default=None
        col.nullable=True
        args.append(col)
    args.append(Column('action', String(12)))
    args.append(Column('aud_id', Integer, primary_key=True))
#    args.append(Column('aud_user_id', Integer, ForeignKey('tUser.user_id')))
    args.append(Column('aud_time', TIMESTAMP, server_default=text('LOCALTIMESTAMP')))
    return Table(table.name+"_aud", table.metadata, *args, extend_existing=True)

if __name__ == '__main__':
    logging.basicConfig(level='INFO')
    run()
   # create_triggers()
