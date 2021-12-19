import typing as tp
from compgraph import operations as ops, external_sort as sort

import copy

Operations = tp.Union[ops.Operation, tp.Callable[[ops.TRowsGenerator, tp.Any], ops.TRowsGenerator]]


class Graph:
    """Computational graph implementation"""

    # new
    __slots__ = (
        "source_type",
        "source",
        "parser",
        "fabric",
        "operations",
        "return_list"
    )

    source: str
    source_type: str
    parser: tp.Callable[[str], ops.TRow]
    operations: tp.List[Operations]
    fabric: tp.Callable[..., tp.Callable[[str, tp.Callable[[str], ops.TRow]], ops.TRowsGenerator]]
    return_list: bool

    def __init__(self, operations: tp.List[Operations]) -> None:
        self.operations = operations

    # ----

    @staticmethod
    def graph_from_iter(name: str) -> 'Graph':
        """Construct new graph which reads data from row iterator (in form of sequence of Rows
        from 'kwargs' passed to 'run' method) into graph data-flow
        Use ops.ReadIterFactory
        :param name: name of kwarg to use as data source
        """
        graph = Graph([])
        setattr(graph, "return_list", True)
        setattr(graph, "source_type", "generator")
        setattr(graph, "source", name)
        return graph

    @staticmethod
    def graph_from_file(filename: str, parser: tp.Callable[[str], ops.TRow]) -> 'Graph':
        """Construct new graph extended with operation for reading rows from file
        Use ops.Read
        :param filename: filename to read from
        :param parser: parser from string to Row
        """

        def fabric() -> tp.Callable[[str, tp.Callable[[str], ops.TRow]], ops.TRowsGenerator]:
            def generator(filename: str, parser: tp.Callable[[str], ops.TRow]) -> ops.TRowsGenerator:
                with open(filename, "r") as file:
                    for string in file:
                        yield parser(string)

            return generator

        graph = Graph([])
        setattr(graph, "source_type", "file")
        setattr(graph, "source", filename)
        setattr(graph, "parser", parser)
        setattr(graph, "fabric", fabric)
        return graph

    def __copy__(self) -> 'Graph':
        """Creates a copy of Graph class"""
        cls = self.__class__
        object = cls.__new__(cls)
        for elem in self.__slots__:
            try:
                attr = getattr(self, elem)
            except AttributeError:
                pass
            else:
                setattr(object, elem, copy.copy(attr))
        return object

    def map(self, mapper: ops.Mapper) -> 'Graph':
        """Construct new graph extended with map operation with particular mapper
        :param mapper: mapper to use
        """
        graph = copy.copy(self)
        graph.operations.append(ops.Map(mapper))
        return graph

    def reduce(self, reducer: ops.Reducer, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with reduce operation with particular reducer
        :param reducer: reducer to use
        :param keys: keys for grouping
        """
        graph = copy.copy(self)
        graph.operations.append(ops.Reduce(reducer, tuple(keys)))
        return graph

    def sort(self, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with sort operation
        :param keys: sorting keys (typical is tuple of strings)
        """
        graph = copy.copy(self)
        graph.operations.append(sort.ExternalSort(keys))
        return graph

    def join(self, joiner: ops.Joiner, join_graph: 'Graph', keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with join operation with another graph
        :param joiner: join strategy to use
        :param join_graph: other graph to join with
        :param keys: keys for grouping
        """
        graph = copy.copy(self)

        def join_op(left_generator: ops.TRowsGenerator, **right_kwargs: tp.Any) -> ops.TRowsGenerator:
            right_generator = join_graph.run(**right_kwargs)
            return ops.Join(joiner, keys=keys)(left_generator, right_generator)

        graph.operations.append(join_op)
        return graph

    def run(self, **kwargs: tp.Any) -> ops.TRowsIterable:
        """Single method to start execution; data sources passed as kwargs"""

        current_generator = kwargs[
            self.source]() if self.source_type == "generator" \
            else self.fabric()(self.source, self.parser)

        for operation in self.operations:
            new_kwargs = kwargs.copy()
            current_generator = operation(current_generator, **new_kwargs)
        return list(current_generator)
