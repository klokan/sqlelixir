from . import SQLElixir as Base


class SQLElixir(Base):

    def __init__(self, app=None, db=None):
        super().__init__()
        if app is not None:
            self.init_app(app, db)

    def init_app(self, app, db=None):
        app.extensions['sqlelixir'] = self
        if db is not None:
            self.metadata = db.metadata
        self.register_package(app.import_name)
