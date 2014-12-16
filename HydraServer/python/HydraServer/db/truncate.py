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
        Table
from sqlalchemy.engine import reflection
from sqlalchemy.orm import sessionmaker
import logging
from mysql.connector.connection import MySQLConnection
from HydraLib import config
import os
import datetime
from multiprocessing import Process
from sqlalchemy.ext.declarative import declarative_base
from decimal import Decimal
from sqlalchemy import and_

Base = declarative_base()


engine_name = config.get('mysqld', 'url')

def connect_mysql():
    """
        return an inspector object
    """
    MySQLConnection.get_characterset_info = MySQLConnection.get_charset

    db = create_engine(engine_name)
    db.echo = True
    db.connect()
    
    return db

sqlite_engine = "sqlite:///%s"%(config.get('sqlite', 'backup_url'))

def connect_sqlite():

    db = create_engine(sqlite_engine, echo=True)
    db.connect()
    return db

def truncate_all_audit_tables():
    logging.info("Starting truncation")

    mysql_db = connect_mysql()
    sqlite_db = connect_sqlite()

    # create a configured "Session" class
    session1 = sessionmaker(bind=mysql_db)
    session2 = sessionmaker(bind=sqlite_db)

    # create a Session
    mysql_session = session1()
    sqlite_session = session2() 

    mysql_metadata = MetaData(mysql_db)
    sqlite_metadata = MetaData(sqlite_db)

    insp = reflection.Inspector.from_engine(mysql_db)

    tables = {}
    audit_tables = {}
    for table_name in insp.get_table_names():
        table = Table(table_name, mysql_metadata, autoload=True)
        if table_name.endswith('_aud'):
            audit_tables[table_name] = table
        else:
            tables[table_name] = table

    for t in tables.keys():
       table       = tables[t]
       audit_table = audit_tables.get("%s_aud"%(t))
       if audit_table is None:
           logging.warning("Audit table for %s does not exist"%(table.name))
           continue
       export_table_to_sqlite(mysql_session, sqlite_session, sqlite_metadata, audit_table)
       truncate_table(mysql_session, table, audit_table)
    
    mysql_session.commit()
    sqlite_session.commit()
    logging.info("Truncation Complete")

def truncate_table(session, table, audit_table):
    """
        @args: session so queries can be made, table so primary key columns
        can be determined and the audit_table to be truncated.
    """
    pk_col_names = [x.name for x in table.primary_key.columns]

    truncated_cols = []
    for audit_col in audit_table.columns:
        if audit_col.name in pk_col_names:
            truncated_cols.append(audit_col)

    rs = session.query(*truncated_cols).group_by(*truncated_cols).all()

    for r in rs:
        args = []
        for arg in truncated_cols:
            args.append(arg == getattr(r, arg.name))
        
        aud_id_col = audit_table.c['aud_id']
        rs = session.query(aud_id_col).filter(and_(*args)).order_by(aud_id_col.desc())[3:]
        aud_ids = [str(r.aud_id) for r in rs]
        if len(aud_ids) > 0:
            delete_sql = "delete from %(table_name)s where aud_id in %(aud_ids)s;"\
                    %{'table_name':audit_table.name,
                      'aud_ids'   : "(%s)"%(','.join(aud_ids),)}
            session.execute(delete_sql)

def export_table_to_csv(session, table, target=None):
    """
        @args: session so queries can be made, table so primary key columns
        can be determined and the audit_table to be truncated.
    """

    if target is None:
        target_dir = os.path.join(config.get('db', 'export_target'))
        target = os.path.join(config.get('db', 'export_target'), table.name)

    if not os.path.exists(target_dir):
        os.mkdir(target_dir)

    if os.path.exists(target):
        target_file      = open(target, 'r+')
    
        rs = session.query(table).all()
        
        entries_in_db = set()
        for r in rs:
            entries_in_db.add("%s"%(r.__repr__()))

        contents = set(target_file.read().split('\n'))
        
        new_data = entries_in_db.difference(contents)

        if len(new_data) > 0:
            target_file.write("%s\n"%(datetime.datetime.now()))
            target_file.write("%s\n"%('.'.join([c.name for c in table.columns])))
            for d in new_data:
                target_file.write("%s\n"%(d))
    else:
        target_file      = open(target, 'w')
        rs = session.query(table).all()
        target_file.write("%s\n"%(datetime.datetime.now()))
        target_file.write("%s\n"%('.'.join([c.name for c in table.columns])))
        for r in rs:
            target_file.write("%s\n"%(r.__repr__()))

def export_table_to_sqlite(mysql_session, sqlite_session, sqlite_metadata, audit_table):
    """
        @args: session so queries can be made, table so primary key columns
        can be determined and the audit_table to be truncated.
    """


    current_data = mysql_session.query(audit_table).all()

    sqlite_table = Table(audit_table.name, sqlite_metadata, autoload=True)

    sqlite_data  =  sqlite_session.query(sqlite_table).all()

    entries_in_mysql_db = set(current_data)
    entries_in_sqlite_db = set(sqlite_data)
        
    new_data = list(entries_in_mysql_db.difference(entries_in_sqlite_db))
    if len(new_data) > 0:
        values = [] 
        for val in new_data:
            row = []
            for i, v in enumerate(val):
                if type(v) == Decimal:
                    row.append(float(v))
                else:
                    row.append(v)
            values.append(tuple(row))

        markers = ','.join('?' * len(new_data[0]))
        colnames = '(%s)'%(','.join(new_data[0].keys()))
        ins = 'INSERT INTO {tablename} {colnames} VALUES ({markers})'
        ins = ins.format(tablename=sqlite_table.name, colnames=colnames, markers=markers)
        res = sqlite_session.connection().execute(ins, values)

        logging.info(res)

def run():
    p = Process(target=truncate_all_audit_tables)
    p.start()
    #p.join()

if __name__ == '__main__':
    logging.basicConfig(level='INFO')
    truncate_all_audit_tables()

