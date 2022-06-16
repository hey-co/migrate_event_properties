from abc import ABC, abstractmethod


class Clean(ABC):

    @abstractmethod
    def clean(self, event_name, property_name):
        pass


class CleanText(Clean):

    def clean(self, event_name, property_name):
        pass


class CleanNumber(Clean):

    def clean(self, event_name, property_name):
        pass


class CleanDate(Clean):

    def clean(self, event_name, property_name):
        pass
