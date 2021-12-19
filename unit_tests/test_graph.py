import typing as tp

from compgraph import Graph, operations
from pytest import approx
from compgraph import algorithms


def test_map() -> None:
    input_stream_name = 'docs'
    text_column = 'text'

    g = Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column))

    docs = [
        {'doc_id': 1, 'text': 'hello, my little WORLD'},
        {'doc_id': 2, 'text': 'Hello, my little little hell'}
    ]

    expected = [
        {'doc_id': 1, 'text': 'hello'},
        {'doc_id': 1, 'text': 'my'},
        {'doc_id': 1, 'text': 'little'},
        {'doc_id': 1, 'text': 'world'},
        {'doc_id': 2, 'text': 'hello'},
        {'doc_id': 2, 'text': 'my'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 2, 'text': 'hell'}
    ]

    result = g.run(docs=lambda: iter(docs))

    assert result == expected


def test_sort() -> None:
    input_stream_name = 'docs'
    text_column = 'text'

    g = Graph.graph_from_iter(input_stream_name) \
        .sort([text_column])

    docs = [
        {'doc_id': 1, 'text': 'hello'},
        {'doc_id': 1, 'text': 'my'},
        {'doc_id': 1, 'text': 'little'},
        {'doc_id': 1, 'text': 'world'},
        {'doc_id': 2, 'text': 'hello'},
        {'doc_id': 2, 'text': 'my'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 2, 'text': 'hell'}
    ]

    expected = [
        {'doc_id': 2, 'text': 'hell'},
        {'doc_id': 1, 'text': 'hello'},
        {'doc_id': 2, 'text': 'hello'},
        {'doc_id': 1, 'text': 'little'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 1, 'text': 'my'},
        {'doc_id': 2, 'text': 'my'},
        {'doc_id': 1, 'text': 'world'}
    ]

    result = g.run(docs=lambda: iter(docs))

    assert result == expected


def test_reduce() -> None:
    input_stream_name = 'docs'
    text_column = 'text'
    count_column = 'count'

    g = Graph.graph_from_iter(input_stream_name) \
        .reduce(operations.Count(count_column), [text_column])

    docs = [
        {'doc_id': 2, 'text': 'hell'},
        {'doc_id': 1, 'text': 'hello'},
        {'doc_id': 2, 'text': 'hello'},
        {'doc_id': 1, 'text': 'little'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 1, 'text': 'my'},
        {'doc_id': 2, 'text': 'my'},
        {'doc_id': 1, 'text': 'world'}
    ]

    expected = [
        {'text': 'hell', 'count': 1},
        {'text': 'hello', 'count': 2},
        {'text': 'little', 'count': 3},
        {'text': 'my', 'count': 2},
        {'text': 'world', 'count': 1}
    ]

    result = g.run(docs=lambda: iter(docs))

    assert result == expected


def test_join() -> None:
    def graph_init(input_stream_name: str,
                   keys: tp.Tuple[str, ...]) -> Graph:
        """Build graph for testing graph_from_iter function"""
        graph_init = Graph.graph_from_iter(input_stream_name)
        return graph_init

    def graph_join(input_stream_name: str,
                   graph2: Graph,
                   keys: tp.Tuple[str, ...]) -> Graph:
        """Build graph for testing simple join function"""
        graph_init = Graph.graph_from_iter(input_stream_name) \
            .join(operations.OuterJoiner(), graph2, keys)

        return graph_init

    data_left = [
        {'player_id': 1, 'username': 'XeroX'},
        {'player_id': 2, 'username': 'jay'},
        {'player_id': 3, 'username': 'Destroyer'},
    ]
    expected = [
        {'player_id': 1, 'username_1': 'XeroX', 'username_2': 'XeroX'},
        {'player_id': 2, 'username_1': 'jay', 'username_2': 'jay'},
        {'player_id': 3, 'username_1': 'Destroyer', 'username_2': 'Destroyer'},
    ]

    graph2 = graph_init('data_left', keys=tuple(['player_id']))
    graph = graph_join('data_left', graph2, keys=tuple(['player_id']))
    result = graph.run(data_left=lambda: iter(data_left))

    assert result == expected


def test_graph_from_iter() -> None:
    def _graph_from_iter(input_stream_name: str) -> Graph:
        """Build graph for testing graph_from_iter"""
        graph_init = Graph.graph_from_iter(input_stream_name)
        return graph_init

    graph = _graph_from_iter('docs')
    docs = [
        {'doc_id': 1, 'text': 'hello, my LitLele WORLD'},
        {'doc_id': 2, 'text': 'Hello, my LitLele little hell'}
    ]
    expected = docs
    result = graph.run(docs=lambda: iter(docs))

    assert result == expected


def test_graph_from_file() -> None:
    pass