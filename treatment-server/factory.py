from Card_Kingdom.ck_factory_main import ck_factory
from Star_City_Games.scg_factory_main import scg_factory

class Factory(object):
    
    def get_factory(self, website):
        factories = {
            'ck': ck_factory(),
            'scg' : scg_factory()
        }

        if website in factories:
            return factories[website]