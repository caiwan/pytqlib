from contextlib import contextmanager
from io import BytesIO, StringIO
from typing import Iterable, Union

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

    def iterate_filenames(self) -> Iterable[str]:
        return (file.filename for file in self._fs.find())

    @contextmanager
    def open(self, filename: str, mode: str = "rb") -> Union[BytesIO, StringIO]:
        if "r" in mode:
            file = self.fs.find_one({"filename": filename})
            if file is None:
                raise FileNotFoundError(f"File '{filename}' not found in GridFS.")
            if "b" in mode:
                buffer = BytesIO(file.read())
            else:
                buffer = StringIO(file.read().decode())
            yield buffer
        elif "w" in mode or "a" in mode:
            buffer = BytesIO()
            yield buffer
            if "a" in mode:
                file = self.fs.find_one({"filename": filename})
                if file is not None:
                    buffer.write(file.read())
            if "w" in mode:
                self.fs.delete(filename)
            self.fs.put(buffer.getvalue(), filename=filename)
        else:
            raise ValueError("Mode must be 'r', 'w', or 'a'.")
