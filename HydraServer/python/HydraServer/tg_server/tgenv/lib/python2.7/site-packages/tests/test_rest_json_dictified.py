from tg import TGController
from tgext.crud import EasyCrudRestController
from .base import CrudTest, Movie, DBSession, metadata, Genre, Actor

import transaction


class TestRestJsonReadDictified(CrudTest):
    """
    Tests for GET requests that enabled dictification, this will rely on
    sprox provider dictify function to resolve also relationships.
     """

    def controller_factory(self):
        class MovieController(EasyCrudRestController):
            model = Movie
            pagination = {'items_per_page': 3}
            json_dictify = True

        class ActorController(EasyCrudRestController):
            model = Actor
            json_dictify = True

        class RestJsonController(TGController):
            movies = MovieController(DBSession)
            actors = ActorController(DBSession)

        return RestJsonController()

    def setUp(self):
        super(TestRestJsonReadDictified, self).setUp()
        genre = Genre(name='action')
        DBSession.add(genre)

        actors = [Actor(name='James Who'), Actor(name='John Doe'), Actor(name='Man Alone')]
        list(map(DBSession.add, actors))

        DBSession.add(Movie(title='First Movie', genre=genre, actors=actors[:2]))
        DBSession.add(Movie(title='Second Movie', genre=genre))
        DBSession.add(Movie(title='Third Movie', genre=genre))
        DBSession.add(Movie(title='Fourth Movie', genre=genre))
        DBSession.add(Movie(title='Fifth Movie'))
        DBSession.add(Movie(title='Sixth Movie'))
        DBSession.flush()
        transaction.commit()

    def test_get_all(self):
        result = self.app.get('/movies.json?order_by=movie_id')
        result = result.json['value_list']
        assert result['total'] == 6, result
        assert result['page'] == 1, result
        assert result['entries'][0]['title'] == 'First Movie', result
        assert len(result['entries'][0]['actors']) == 2, result

        result = self.app.get('/movies.json?page=2&order_by=movie_id')
        result = result.json['value_list']
        assert result['total'] == 6, result
        assert result['page'] == 2, result
        assert result['entries'][0]['title'] == 'Fourth Movie', result

    def test_get_all_filter(self):
        actor = DBSession.query(Actor).first()

        result = self.app.get('/actors.json?movie_id=%s' % actor.movie_id)
        result = result.json['value_list']
        assert result['total'] == 2, result

    def test_get_all___json__(self):
        actor = DBSession.query(Actor).filter(Actor.movie_id!=None).first()
        movie_title = actor.movie.title

        result = self.app.get('/actors.json?movie_id=%s' % actor.movie_id)
        result = result.json['value_list']
        assert result['total'] > 0, result

        for entry in result['entries']:
            assert entry['movie_title'] == movie_title

    def test_get_one(self):
        movie = DBSession.query(Movie).first()
        movie_actors_count = len(movie.actors)

        result = self.app.get('/movies/%s.json' % movie.movie_id)
        result = result.json
        assert result['model'] == 'Movie', result
        assert result['value']['title'] == movie.title
        assert result['value']['movie_id'] == movie.movie_id
        assert len(result['value']['actors']) == movie_actors_count

    def test_get_one___json__(self):
        actor = DBSession.query(Actor).filter(Actor.movie_id!=None).first()
        movie_title = actor.movie.title

        result = self.app.get('/actors/%s.json' % actor.actor_id)
        result = result.json
        assert result['model'] == 'Actor', result
        assert result['value']['name'] == actor.name
        assert result['value']['movie_title'] == movie_title
