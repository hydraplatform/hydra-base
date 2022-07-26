from abc import (
    ABC,
    abstractmethod
)

class DatasetManager(ABC):
    """
    Base class for DatasetManagers.
    This acts as a descriptor in the model.Dataset class and
    subclasses must implement xxx_storage_location() methods
    in addition to the __get__/__set__ descriptor protocol
    """
    @abstractmethod
    def __get__(self, dataset, dtype):
        pass

    @abstractmethod
    def __set__(self, dataset, value):
        pass

    @abstractmethod
    def get_storage_location(self, dataset):
        pass

    @abstractmethod
    def set_storage_location(self, dataset, location):
        pass

    @abstractmethod
    def delete_storage_location(self, dataset):
        pass
