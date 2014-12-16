from sqlalchemy import *
from sqlalchemy.orm import mapper, relation
from sqlalchemy import Table, ForeignKey, Column
from sqlalchemy.types import Integer, Text

from wiki20.model import DeclarativeBase, metadata, DBSession

class Page(DeclarativeBase):
        __tablename__ = 'page'

        id = Column(Integer, primary_key=True)
        pagename = Column(Text, unique=True)
        data = Column(Text)
