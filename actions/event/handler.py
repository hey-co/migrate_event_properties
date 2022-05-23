from validation import Validation
from migration import Migration


class EventHandler:

    def __init__(self):
        self.action: str = 'validate'

    def handler(self):
        if self.action == 'validate':
            self.validate()
        else:
            self.migrate()

    @staticmethod
    def validate():
        return Validation.execute()

    @staticmethod
    def migrate(self):
        return Migration.execute()
