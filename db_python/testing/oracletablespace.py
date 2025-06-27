import logging
logger = logging.getLogger(__name__)

class OracleTablespace:
    def __init__(self, name):
        self.name = name

    def get_name(self):
        return self.name
