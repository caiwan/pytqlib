from contextlib import ExitStack
from typing import Any, Callable, Dict, Type, Optional, Union, Iterable, List, Iterator
from uuid import UUID, uuid4
import dataclasses_json
from marshmallow import Schema

import networkx as nx
from py2neo import Graph, Node, Relationship, NodeMatcher

from tq.database.db import AbstractDao, BaseEntity, transactional, BaseContext


class DataClassJsonGraphNodeMixin(dataclasses_json.DataClassJsonMixin):
    def add_to_graph(self, graph: nx.Graph):
        if self.id is None:
            self.id = uuid4()
        graph.add_node(self.id, **self.to_dict())
        return self.id

    @classmethod
    def from_graph(cls, graph: nx.Graph, id: UUID) -> Optional[Any]:
        if id in graph.nodes:
            person_data = graph.nodes[id]
            return cls(**person_data)
        return None


def graph_node(cls=None):
    def decorator(cls):
        cls.add_to_graph = DataClassJsonGraphNodeMixin.add_to_graph
        cls.from_graph = DataClassJsonGraphNodeMixin.from_graph
        return cls

    if cls is None:
        return decorator
    return decorator(cls)


class Neo4jContext(BaseContext):
    def __init__(self, graph: Graph):
        self._graph = graph
        self._tx = None
        self._exit_stack = None

        self._tx = None

    def __enter__(self):
        self._tx = self._graph.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._tx is None:
            return

        if exc_type:
            self._tx.rollback()
        else:
            self._tx.commit()
        pass

    def sanitize(self, data: Optional[Dict]) -> Optional[Dict]:
        if data is None:
            return None

        if hasattr(data, "items"):
            for key, value in data.items():
                # TODO: Proper UUID -> String conversion (Marsmellow?)

                if isinstance(value, dict):
                    data[key] = self.sanitize(value)

                if isinstance(value, list):
                    data[key] = [self.sanitize(v) for v in value]

        if "id" in data:
            data["id"] = str(data["id"])

        return data

    def desanitize(self, data: Optional[Dict]) -> Optional[Dict]:
        if data is None:
            return None
        if hasattr(data, "items"):
            for key, value in data.items():
                # TODO: String -> UUID

                if isinstance(value, dict):
                    data[key] = self.desanitize(value)

                if isinstance(value, list):
                    data[key] = [self.desanitize(v) for v in value]

        if "id" in data:
            data["id"] = UUID(data["id"])

        return data

    def create(self, obj: BaseEntity) -> UUID:
        matched_node = self.matcher.match(
            obj.__class__.__name__, id=str(obj.id)
        ).first()
        sanitized_data = self.sanitize(obj.to_dict())

        if matched_node:
            for key, value in sanitized_data.items():
                matched_node[key] = value
                self.tx.push(matched_node)
        else:
            node = Node(obj.__class__.__name__, **sanitized_data)
            self.tx.create(node)

        return obj.id

    def find_one(self, **kwargs) -> Optional[Dict]:
        if "id" in kwargs:
            kwargs["id"] = str(kwargs["id"])
        item = self.graph.nodes.match(**kwargs).first()
        return self.desanitize(dict(item)) if item else None

    def iterate_all(self, **kwargs) -> Iterator[Dict]:
        if "id" in kwargs:
            kwargs["id"] = str(kwargs["id"])
        for item in self.graph.nodes.match(**kwargs):
            yield self.desanitize(dict(item))

    def delete(
        self, class_name: str, match: Dict[str, Any], limit: Optional[int] = None
    ) -> int:
        if "id" in match:
            match["id"] = str(match["id"])

        nodes = self.matcher.match(class_name, **match)
        limit = limit or len(nodes)
        count = 0
        for node in nodes:
            if count >= limit:
                break
            self.tx.delete(node)
            count += 1
        return count

    # def store_graph(self, G: nx.Graph):
    #     # TODO: Merge with existing graph
    #     for node, data in G.nodes(data=True):
    #         n = Node(self.key_prefix, **data)
    #         self._graph.create(n)
    #         G.nodes[node]["node"] = n

    #     for src, tgt, data in G.edges(data=True):
    #         u = G.nodes[src].get("node")
    #         v = G.nodes[tgt].get("node")
    #         rel = Relationship(u, "CONNECTS", v, **data)
    #         self._graph.create(rel)

    # def restore_graph(self) -> nx.Graph:
    #     G = nx.Graph()

    #     for node in self._graph.nodes.match():
    #         G.add_node(node.identity, **dict(node))

    #     for rel in self._graph.relationships.match():
    #         G.add_edge(rel.start_node.identity, rel.end_node.identity, **dict(rel))

    #     return G

    def _run_transaction(self, fn: Callable[..., Any], is_subcontext: bool = False):
        if is_subcontext:
            return fn()

        # TODO: Config Attempt count
        for attempt in range(3):
            try:
                with self:
                    return fn()
            except Exception as e:
                print(f"Attempt {attempt} failed with {e}")
                if attempt == 2:
                    raise e

    @property
    def graph(self) -> Graph:
        return self._graph

    @property
    def tx(self):
        if self._tx is None:
            raise RuntimeError("Transaction not started")
        return self._tx

    @property
    def matcher(self) -> NodeMatcher:
        return NodeMatcher(self._graph)


class Neo4jDao(AbstractDao):
    def __init__(
        self,
        schema: Type[Schema],
        graph: Graph,
        key_prefix: str = "",
    ):
        # TODO: Register multiple schemas
        super().__init__(schema, key_prefix)  # TODO: None of these are used
        self._graph = graph

    @transactional
    def create_or_update(self, obj: BaseEntity, ctx: Neo4jContext) -> UUID:
        if obj.id is None:
            obj.id = uuid4()
        return ctx.create(obj)

    @transactional
    def bulk_create_or_update(
        self, objs: Iterable[BaseEntity], ctx: Neo4jContext
    ) -> List[UUID]:
        return list([self.create_or_update(obj, ctx=ctx) for obj in objs])

    @transactional
    def get_entity(
        self, id: Optional[Union[UUID, str]], ctx: Neo4jContext
    ) -> BaseEntity:
        obj = ctx.find_one(id=id)
        return self.schema.from_dict(obj) if obj else None

    @transactional
    def get_all(self, ctx: Neo4jContext) -> List[BaseEntity]:
        return list(self.iterate_all(ctx=ctx))

    @transactional
    def iterate_all(self, ctx: Neo4jContext) -> Iterator[BaseEntity]:
        for item in ctx.iterate_all():
            yield self.schema.from_dict(item)

    @transactional
    def iterate_all_keys(self, ctx: Neo4jContext) -> Iterator[BaseEntity]:
        for item in ctx.iterate_all():
            yield item["id"]

    @transactional
    def delete(self, id: Optional[Union[UUID, str]], ctx: Neo4jContext) -> int:
        return ctx.delete(self.schema.__name__, match={"id": id}, limit=1)

    @transactional
    def make_connection(
        self,
        src: BaseEntity,
        tgt: BaseEntity,
        ctx: Neo4jContext,
        connection_type: str = "",
        connection_data: Optional[BaseEntity] = None,
    ):
        pass

    def _create_context(self, ctx: Optional[BaseContext] = None) -> BaseContext:
        return Neo4jContext(self._graph)
