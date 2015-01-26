from tg import TGApp, AppConfig

from webtest import TestApp

from sqlalchemy import Column, ForeignKey, Integer, String, Text, Date
from zope.sqlalchemy import ZopeTransactionExtension
from sqlalchemy.orm import scoped_session, sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from webob import UTC

maker = sessionmaker(autoflush=True, autocommit=False,
                     extension=ZopeTransactionExtension())
DBSession = scoped_session(maker)
DeclarativeBase = declarative_base()
metadata = DeclarativeBase.metadata

MODIFICATION_DATE = datetime(2010, 1, 1, 12, 0, tzinfo=UTC)

class Genre(DeclarativeBase):
    __tablename__ = "genres"

    genre_id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)


class Movie(DeclarativeBase):
    __tablename__ = "movies"

    movie_id = Column(Integer, primary_key=True)
    title = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)
    release_date = Column(Date, nullable=True, default=datetime.utcnow)

    genre_id = Column(Integer, ForeignKey(Genre.genre_id), nullable=True)
    genre = relationship(Genre)

    @property
    def updated_at(self):
        return MODIFICATION_DATE

class Actor(DeclarativeBase):
    __tablename__ = "actors"

    actor_id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)

    movie_id = Column(Integer, ForeignKey(Movie.movie_id), nullable=True)
    movie = relationship(Movie, backref='actors')

    def __json__(self):
        return {'name':self.name,
                'movie_id':self.movie_id,
                'actor_id':self.actor_id,
                'movie_title':self.movie and self.movie.title or None}

class FakeModel(object):
    __file__ = 'model.py'

    movie = Movie
    DBSession = DBSession

    def init_model(self, engine):
        if metadata.bind is None:
            DBSession.configure(bind=engine)
            metadata.bind = engine


class FakePackage(object):
    __file__ = 'package.py'
    __name__ = 'tests'

    model = FakeModel()


class CrudTest(object):
    def setUp(self):
        self.root_controller = self.controller_factory()
        conf = AppConfig(minimal=True, root_controller=self.root_controller)
        conf.package = FakePackage()
        conf.model = conf.package.model
        conf.use_dotted_templatenames = True
        conf.renderers = ['json', 'jinja', 'mako']
        conf.default_renderer = 'jinja'
        conf.use_sqlalchemy = True
        conf.paths = {'controllers':'tests',
                      'templates':['tests']}
        conf.disable_request_extensions = False
        conf.prefer_toscawidgets2 = True
        conf.use_transaction_manager = True
        conf['sqlalchemy.url'] = 'sqlite:///:memory:'

        self.app = TestApp(conf.make_wsgi_app())

        metadata.create_all()

    def tearDown(self):
        metadata.drop_all()
