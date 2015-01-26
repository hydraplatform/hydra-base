from tg import TGController
import transaction
from tgext.crud import EasyCrudRestController
from .base import CrudTest, Movie, DBSession, metadata, Actor, Genre


class TestCrudHTML(CrudTest):
    def controller_factory(self):
        class MovieController(EasyCrudRestController):
            model = Movie

        class CrudHTMLController(TGController):
            movies = MovieController(DBSession)

        return CrudHTMLController()

    def test_post(self):
        result = self.app.post('/movies/', params={'title':'Movie Test'}, status=302)

        movie = DBSession.query(Movie).first()
        assert movie is not None

        assert result.headers['Location'] == 'http://localhost/movies/', result

    def test_post_validation(self):
        result = self.app.post('/movies/')

        assert '<form' in result, result
        assert 'Please enter a value' in result, result
        assert DBSession.query(Movie).first() is None

    def test_post_validation_dberror(self):
        metadata.drop_all(tables=[Movie.__table__])

        result = self.app.post('/movies/', params={'title':'Movie Test'})
        assert '<form' in result, result
        assert '(OperationalError)' in result, result

    def test_search(self):
        result = self.app.get('/movies/')
        assert 'id="crud_search_field"' in result, result

    def test_search_disabled(self):
        self.root_controller.movies.search_fields = False
        result = self.app.get('/movies/')
        assert 'id="crud_search_field"' not in result, result

    def test_search_some(self):
        self.root_controller.movies.search_fields = ['title']
        result = self.app.get('/movies/')
        assert 'id="crud_search_field"' in result, result
        assert 'value="title"' in result, result
        assert 'value="genre"' not in result, result


class TestCrudHTMLSearch(CrudTest):
    def controller_factory(self):
        class MovieController(EasyCrudRestController):
            model = Movie

        class CrudHTMLController(TGController):
            movies = MovieController(DBSession)

        return CrudHTMLController()

    def setUp(self):
        super(TestCrudHTMLSearch, self).setUp()
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

    def test_search_no_filters(self):
        result = self.app.get('/movies/')
        assert 'First Movie' in result
        assert 'Second Movie' in result

    def test_search_by_text(self):
        result = self.app.get('/movies/?title=First%20Movie')
        assert 'First Movie' in result
        assert 'Second Movie' not in result

    def test_search_by_substring(self):
        self.root_controller.movies.substring_filters = ['title']
        result = self.app.get('/movies/?title=d%20Movie')
        assert 'First Movie' not in result
        assert 'Second Movie' in result
        assert 'Third Movie' in result
        assert 'Fourth Movie' not in result

    def test_search_relation_by_id(self):
        result = self.app.get('/movies/?actors=1')
        assert 'First Movie' in result
        assert 'Second Movie' not in result

    """
    def test_search_relation_by_text(self):
        result = self.app.get('/movies/?genre=action')
        assert 'First Movie' in result
        assert 'Second Movie' in result
        assert 'Fifth Movie' not in result
    """