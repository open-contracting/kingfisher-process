

class CollectionModel:

    def __init__(self, database_id=None, source_id=None, data_version=None, sample=None, check_data=None,
                 check_older_data_with_schema_version_1_1=None):
        self.database_id = database_id
        self.source_id = source_id
        self.data_version = data_version
        self.sample = sample
        self.check_data = check_data
        self.check_older_data_with_schema_version_1_1 = check_older_data_with_schema_version_1_1


class FileModel:

    def __init__(self, database_id=None, filename=None):
        self.database_id = database_id
        self.filename = filename


class FileItemModel:

    def __init__(self, database_id=None):
        self.database_id = database_id
