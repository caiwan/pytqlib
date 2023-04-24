import gridfs

from tq.database.db import AbstractFsDao


class GridFsDao(AbstractFsDao):
    def __init__(self, client):
        self._client = client
        self._db = self._client.get_default_database()
        self._fs = gridfs.GridFS(self._db)

    def store(self, data, filename):
        return self._fs.put(data, filename=filename)

    def load(self, file_id):
        return self._fs.get(file_id).read()

    def delete(self, file_id):
        return self._fs.delete(file_id)
