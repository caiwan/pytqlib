import io
from contextlib import contextmanager
from typing import Any, Iterable, Union
from uuid import UUID

import pymongo
from bson.objectid import ObjectId
from gridfs import GridFS, GridFSBucket
from gridfs.errors import NoFile

from tq.database.db import AbstractFsDao


class SimpleGridFsDao(AbstractFsDao):
    def __init__(self, client):
        self._client = client
        self._db = self._client.get_default_database()
        self._fs = GridFS(self._db)

    def store(self, filename: str, data: bytes) -> str:
        file_id = self._fs.put(data, filename=filename)
        return str(file_id)

    def load_by_id(self, file_id: str) -> bytes:
        return self._fs.get(ObjectId(file_id)).read()

    def delete_by_id(self, file_id: str) -> bool:
        return self._fs.delete(ObjectId(file_id))

    def load(self, filename: str) -> bytes:
        file_id = self.find_file_id(filename)
        return self.load_by_id(file_id) if file_id else None

    def delete(self, filename: str) -> bool:
        file_id = self.find_file_id(filename)
        return self._fs.delete(file_id) if file_id else False

    def iterate_filenames(self) -> Iterable[str]:
        return self._fs.list()

    def find_file_id(self, filename: str) -> str:
        file = self._fs.find_one({"filename": filename})
        return str(file._id) if file else None

    @contextmanager
    def open(self, filename: str, mode: str = "rb") -> Union[io.BytesIO, io.StringIO]:
        raise NotImplementedError()


class BucketGridFsDao(AbstractFsDao):
    def __init__(self, client):
        self._client = client
        self._db = self._client.get_default_database()
        self._bucket = GridFSBucket(self._db)

    def store(self, filename: str, data: Any) -> UUID:
        if isinstance(data, io.IOBase):
            file_id = self._bucket.upload_from_stream(filename, data)
        elif isinstance(data, bytes):
            with io.BytesIO(data) as stream:
                file_id = self._bucket.upload_from_stream(filename, stream)
        elif isinstance(data, str):
            with io.StringIO(data) as stream:
                file_id = self._bucket.upload_from_stream(filename, stream)
        return str(file_id)

    def load_by_id(self, file_id: str) -> bytes:
        file_obj = self._bucket.open_download_stream(ObjectId(file_id))
        return file_obj.read()

    def delete_by_id(self, file_id: UUID) -> bool:
        try:
            self._bucket.delete(ObjectId(file_id))
            return True
        except pymongo.errors.NoMatchingDocument:
            return False

    def load(self, filename: str) -> bytes:
        file_obj = self._bucket.open_download_stream_by_name(filename)
        return file_obj.read()

    def delete(self, filename: str) -> bool:
        try:
            file_id = self.find_file_id(filename)
            if not file_id:
                return False
            self._bucket.delete(ObjectId(file_id))
            return True
        except NoFile:
            return False

    def iterate_filenames(self) -> Iterable[str]:
        return (file.filename for file in self._bucket.find())

    def find_file_id(self, filename: str) -> str:
        file = self._bucket.find({"filename": filename})
        first_item = next(file, None)
        # if next(file, None):
        #     return None
        return str(first_item._id) if first_item else None

    @contextmanager
    def open(self, filename: str, mode: str = "rb") -> io.IOBase:
        # 1. Read
        if "r" in mode:
            file_obj = self._bucket.open_download_stream_by_name(filename)
            yield file_obj

        # 2. Write
        elif "w" in mode or "a" in mode:
            # Create file
            if "a" in mode:
                # TODO: Add Append mode
                # file_obj = self._bucket.open_download_stream_by_name(filename)
                with self._bucket.open_upload_stream(filename=filename) as file_obj:
                    yield file_obj

            else:
                self._bucket.upload_from_stream(filename, io.BytesIO())
                with self._bucket.open_upload_stream(filename=filename) as file_obj:
                    yield file_obj
            # raise ValueError("Write mode not supported")


GridFsDao = BucketGridFsDao
