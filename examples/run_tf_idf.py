import itertools
import json
import os

from compgraph import algorithms as graphs
from compgraph import operations as ops

dir_path = os.path.dirname(os.path.realpath(__file__))
resource_path = os.path.join(os.path.split(dir_path)[0], 'resources')


def parser(line: str) -> ops.TRow:
    return json.loads(line)


def tf_idf() -> None:
    graph = graphs.inverted_index_graph('texts', doc_column='doc_id', text_column='text', result_column='tf_idf')
    filename = os.path.join(resource_path, "text_corpus.txt")
    graph = graph.graph_from_file(filename=filename, parser=parser)

    result = graph.run()

    for res in itertools.islice(result, 5):
        print(res, '\n\n')
    print("experiment successfully finished")


if __name__ == "__main__":
    print(tf_idf())
