from abc import ABC, abstractmethod

class website_functions(ABC):
    
    @abstractmethod
    def load_non_foil_data(self,session,input_date):
        """ loads data into the non-foil table for the website"""
    
    @abstractmethod
    def load_foil_data(self, session, input_date):
        """ load data into the foil for the website"""