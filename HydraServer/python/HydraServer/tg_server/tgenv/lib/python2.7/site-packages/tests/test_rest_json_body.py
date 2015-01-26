from tg import TGController
from tgext.crud import EasyCrudRestController
from .base import CrudTest, Movie, DBSession, metadata, Genre, Actor

import transaction


class TestRestJsonEditCreateJsonBody(CrudTest):
    """Tests for submitting POST & PUT values through a JSON body instead of parameters"""

    def controller_factory(self):
        class MovieController(EasyCrudRestController):
            model = Movie

        class RestJsonController(TGController):
            movies = MovieController(DBSession)

        return RestJsonController()

    def test_post(self):
        result = self.app.post_json('/movies.json', params={'title':'Movie Test'})

        movie = DBSession.query(Movie).first()
        assert movie is not None, result

        assert movie.movie_id == result.json['value']['movie_id']

    def test_post_validation(self):
        result = self.app.post_json('/movies.json', params={'title':''}, status=400)
        assert result.json['title'] is not None #there is an error for required title
        assert result.json['description'] is None #there isn't any error for optional description

        assert DBSession.query(Movie).first() is None

    def test_put(self):
        result = self.app.post_json('/movies.json', params={'title':'Movie Test'})
        movie = result.json['value']

        result = self.app.put_json('/movies/%s.json' % movie['movie_id'],
                              params={'title':'New Title'})
        assert result.json['value']['title'] == 'New Title'

    def test_put_validation(self):
        result = self.app.post_json('/movies.json', params={'title':'Movie Test'})
        movie = result.json['value']

        result = self.app.put_json('/movies/%s.json' % movie['movie_id'],
                              params={'title':''}, status=400)
        assert result.json['title'] is not None #there is an error for required title
        assert result.json['description'] is None #there isn't any error for optional description

        movie = DBSession.query(Movie).first()
        assert movie.title == 'Movie Test'

    def test_put_relationship(self):
        result = self.app.post_json('/movies.json', params={'title':'Movie Test'})
        movie = result.json['value']

        actors = [Actor(name='James Who'), Actor(name='John Doe'), Actor(name='Man Alone')]
        list(map(DBSession.add, actors))
        DBSession.flush()
        actor_ids = [actor.actor_id for actor in actors[:2]]
        transaction.commit()

        result = self.app.put_json('/movies/%s.json' % movie['movie_id'],
                              params={'title':'Movie Test',
                                      'actors':actor_ids})
        assert len(result.json['value']['actors']) == 2, result

        assert DBSession.query(Actor).filter_by(movie_id=movie['movie_id']).count() == 2
