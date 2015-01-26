from sqlalchemy.orm import Session
from sqlalchemy import create_engine
import HydraAlchemy

if __name__ == '__main__':
    import pudb; pudb.set_trace()
    
    e = create_engine("sqlite://", echo=True)

    HydraAlchemy.Base.metadata.create_all(e)

    DBSession = Session(e)

    test = HydraAlchemy.Attr(attr_name="test")
    e.add(test)
