

class CollectionModel:

    def __init__(self, database_id=None, source_id=None, data_version=None, sample=None):
        self.database_id = database_id
        self.source_id = source_id
        self.data_version = data_version
        self.sample = sample


class FileModel:

    def __init__(self, database_id=None, filename=None):
        self.database_id = database_id
        self.filename = filename


class FileItemModel:

    def __init__(self, database_id=None):
        self.database_id = database_id
