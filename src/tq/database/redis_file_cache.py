import logging
from contextlib import contextmanager
from dataclasses import dataclass
from io import IOBase
from typing import Any, Iterable, List
from uuid import UUID

from tq.database.db import AbstractFsDao, BaseEntity
from tq.database.redis_dao import transactional

logger = logging.getLogger(__name__)


DEFAULT_CHUNK_SIZE = 1024**2


@dataclass
class FileAttachment(BaseEntity):
    segemnts: List[UUID]


# def read_file_in_chunks(path: pathlib.Path, chunksize=DEFAULT_CHUNK_SIZE) -> bytes:
#     with open(path, "rb") as f:
#         for chunk in iter(lambda: bytes(f.read(chunksize)), b""):
#             yield chunk


# def write_file_in_chunks(path: pathlib.Path, chunks: Iterable):
#     with open(path, "wb") as f:
#         for chunk in chunks:
#             f.write(chunk)


class RedisFileFsDao(AbstractFsDao):
    def __init__(self, client):
        self._client = client
        # self._db = self._client.get_default_database()
        # self._fs = gridfs.GridFS(self._db)

    # @transactional
    # def store(self, filename: str, data: Any) -> str:
    #     chunk_ctx = self._chunk_ctx(ctx)
    #     chunk_ids = [
    #         chunk_ctx.set(uuid4(), chunk) for chunk in read_file_in_chunks(path)
    #     ]
    #     file_id = uuid4()
    #     for chunk_id in chunk_ids:
    #         ctx.list_push(file_id, str(chunk_id))
    #     return file_id

    def load(self, filename: str) -> bytes:
        pass

    def delete(self, filename: str) -> bool:
        pass

    def load_by_id(self, file_id: str) -> bytes:
        pass

    def delete_by_id(self, file_id: str) -> bool:
        pass

    def iterate_filenames(self) -> Iterable[str]:
        pass

    def find_file_id(self, filename: str) -> str:
        pass

    @contextmanager
    def open(self, filename: str, mode: str = "rb") -> IOBase:
        pass


# class FileDao(BaseDao):
#     def __init__(self, redis_db):
#         super().__init__(redis_db, FileAttachment.schema(), key_prefix="file")

#     def _chunk_ctx(self, ctx: DaoContext) -> DaoContext:
#         return DaoContext(ctx.db, "file_chunk")

#     @transactional
#     def pull_from_disk(self, ctx: DaoContext, path: pathlib.Path) -> UUID:
#         # file -> db
#         logger.debug(f"Pulling file {path} from disk to db")


#     @transactional
#     def push_to_disk(self, ctx: DaoContext, id: UUID, target: pathlib.Path):
#         # file <- db
#         logger.debug(f"Pushing {id} to disk {target}")
#         if ctx.is_exists(id):
#             chunk_ctx = self._chunk_ctx(ctx)
#             chunks = [chunk_ctx.get(UUID(chunk_id.decode())) for chunk_id in ctx.iter_all_from_list(id)]
#             write_file_in_chunks(target, chunks)
#             return True
#         return False

#     @contextmanager
#     def as_tempfile(self, id: UUID, **kwargs):
#         logger.debug(f"Crating file {id} as tempfile")
#         with tempfile.NamedTemporaryFile(**kwargs) as tmp:
#             if self.push_to_disk(id, pathlib.Path(tmp.name)):
#                 yield tmp
#             else:
#                 # TODO: Error
#                 pass

#     @transactional
#     def delete_file(self, ctx: DaoContext, id: UUID):
#         if ctx.is_exists(id):
#             ctx.delete(id)
#             return True
#         return False

#     @transactional
#     def cleanup_orphans(self, ctx: DaoContext) -> int:
#         chunk_ctx = self._chunk_ctx(ctx)
#         orphan_chunk_ids = self.find_orphans()
#         for orphan_chunk_id in orphan_chunk_ids:
#             chunk_ctx.delete(orphan_chunk_id)
#             chunk_ctx.trigger_db_cleanup()
#         return len(orphan_chunk_ids)

#     @transactional
#     def find_orphans(self, ctx: DaoContext) -> Set[UUID]:
#         chunk_ctx = self._chunk_ctx(ctx)
#         chunk_ids = set(chunk_id for chunk_id in chunk_ctx.iterate_all_keys())

#         for file_id in ctx.iterate_all_keys():
#             for chunk_id in ctx.iter_all_from_list(file_id):
#                 if chunk_id not in chunk_ids:
#                     logging.warn(f"Corrupted file={file_id} missing chunk={chunk_id}")
#                 else:
#                     chunk_ids.remove(chunk_id)

#         return chunk_ids
