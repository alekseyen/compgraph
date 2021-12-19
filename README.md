# Library for graph computation with constant memory consumption

## Calculation graph interface

The computation graph consists of entry points for data and operations on them.

 ```python
 graph = Graph.graph_from_iter('texts') \
     .map(operations.FilterPunctuation('text')) \
     .map(operations.LowerCase('text')) \
     .map(operations.Split('text')) \
     .sort(['text']) \
     .reduce(operations.Count('count'), ['text']) \
     .sort(['count', 'text'])
 ```

## How to use

### How to install
```bash
pip install -e compgraph --force-reinstall
```

```bash
setup sdist bdist_wheel
```

### How to write own computation graph

- Create an graph object, for example: `graph = graphs.word_count_graph ('docs', text_column = 'text', count_column = 'count')`
- Supply an iterator to the input for a table like `tp.List (tp.Dict [str, tp.Any])`: `result = graphs.run (docs = lambda: iter (docs))`

```(python)
def parser(line: str) -> ops.TRow:
    return json.loads(line)


def run_word_count() -> None:
    graph = graphs.word_count_graph('docs', text_column='text', count_column='count')

    docs = graph.graph_iter_from_file(filename="./compgraph/resource/text_corpus.txt", parser=parser)

    result = graph.run_iter(docs=lambda: docs)
    for res in itertools.islice(result, 5):
        print(res)

    print("experiment successfully finished")
```

You can find some examples at [examples](examples) folder