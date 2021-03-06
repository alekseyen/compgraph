from compgraph import Graph, operations
import typing as tp


def word_count_graph(input_stream_name: str, text_column: str = 'text', count_column: str = 'count') -> Graph:
    """Constructs graph which counts words in text_column of all rows passed"""
    return Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([text_column]) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort([count_column, text_column])


def inverted_index_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
                         result_column: str = 'tf_idf') -> Graph:
    """Constructs graph which calculates td-idf for every word/document pair"""
    word_graph = Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column))

    count_graph = Graph.graph_from_iter(input_stream_name) \
        .reduce(operations.Count('doc_count'), [])

    idf_graph = word_graph \
        .sort([doc_column, text_column]) \
        .reduce(operations.FirstReducer(), [doc_column, text_column]) \
        .sort([text_column]) \
        .reduce(operations.Count('num_word_entries'), [text_column]) \
        .join(operations.InnerJoiner(), count_graph, []) \
        .map(operations.Idf('doc_count', 'num_word_entries', text_column, 'idf')) \
        .sort([text_column])  # [word, idf]

    tf_graph = word_graph \
        .sort([doc_column]) \
        .reduce(operations.TermFrequency(text_column, 'tf'), [doc_column]) \
        .sort([text_column])  # [doc_id, word, tf]

    tf_idf_graph = tf_graph \
        .join(operations.InnerJoiner(), idf_graph, [text_column]) \
        .map(operations.Product(['tf', 'idf'], result_column)) \
        .map(operations.Project([result_column, doc_column, text_column])) \
        .sort([text_column]) \
        .reduce(operations.TopN(result_column, 3), [text_column])

    return tf_idf_graph


def pmi_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
              result_column: str = 'pmi') -> Graph:
    """Constructs graph which gives for every document the top 10 words ranked by pointwise mutual information"""

    def is_enough_length(row: tp.Dict[str, tp.Any]) -> bool:
        return len(row[text_column]) > 4

    word_graph = Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .map(operations.Filter(is_enough_length)) \
        .sort([doc_column, text_column]) \
        .reduce(operations.SafeCount('num_entries'), [doc_column, text_column]) \
        .map(operations.Filter(lambda row: row['num_entries'] >= 2))

    tf_graph = word_graph \
        .sort([doc_column]) \
        .reduce(operations.TermFrequency(text_column, 'tf'), [doc_column]) \
        .sort([text_column])  # [doc_id, word, tf]

    tf_graph_total = word_graph \
        .reduce(operations.TermFrequency(text_column, 'tf_total'), []) \
        .sort([text_column])  # [word, tf_total]

    graph_pmi = tf_graph \
        .join(operations.InnerJoiner(), tf_graph_total, [text_column]) \
        .map(operations.Pmi('tf', 'tf_total', result_column)) \
        .map(operations.Project([doc_column, text_column, result_column])) \
        .sort([doc_column]) \
        .reduce(operations.TopN(result_column, 10), [doc_column])

    return graph_pmi


def yandex_maps_graph(input_stream_name_time: str, input_stream_name_length: str,
                      enter_time_column: str = 'enter_time', leave_time_column: str = 'leave_time',
                      edge_id_column: str = 'edge_id', start_coord_column: str = 'start', end_coord_column: str = 'end',
                      weekday_result_column: str = 'weekday', hour_result_column: str = 'hour',
                      speed_result_column: str = 'speed') -> Graph:
    """Constructs graph which measures average speed in km/h depending on the weekday and hour"""

    length = Graph.graph_from_iter(input_stream_name_length) \
        .map(operations.ProcessLength(start_coord_column, end_coord_column, 'length')) \
        .sort([edge_id_column])

    time = Graph.graph_from_iter(input_stream_name_time) \
        .map(operations.ProcessTime(enter_time_column,
                                    leave_time_column,
                                    'time',
                                    weekday_result_column,
                                    hour_result_column)) \
        .sort([edge_id_column]) \
        .join(operations.InnerJoiner(), length, [edge_id_column]) \
        .sort([weekday_result_column, hour_result_column]) \
        .reduce(operations.MultipleSum(['time', 'length']), [weekday_result_column, hour_result_column])

    return time.map(operations.ProcessSpeed('length', 'time', speed_result_column)) \
        .map(operations.Project([weekday_result_column, hour_result_column, speed_result_column])) \
        .sort([weekday_result_column, hour_result_column])

# def yandex_maps_graph(input_stream_name_time: str, input_stream_name_length: str,
#                       enter_time_column: str = 'enter_time', leave_time_column: str = 'leave_time',
#                       edge_id_column: str = 'edge_id', start_coord_column: str = 'start', end_coord_column: str = 'end',
#                       weekday_result_column: str = 'weekday', hour_result_column: str = 'hour',
#                       speed_result_column: str = 'speed', source_is_file: bool = False, parser: tp.Any = None) -> Graph:
#     """Constructs graph which measures average speed in km/h depending on the weekday and hour"""
#     time_delta_column = "time"
#     length_column = "length"
#     speed_column = "speed at moment"
#
#     if source_is_file:
#         graph_length_init = Graph.graph_from_file(input_stream_name_length, parser) \
#
#         graph_time_init = Graph.graph_from_file(input_stream_name_time, parser) \
#
#     else:
#         graph_length_init = Graph.graph_from_iter(input_stream_name_length)
#
#         graph_time_init = Graph.graph_from_iter(input_stream_name_time) \
#
#     graph_length = graph_length_init.map(operations.Len_mapper(start_coord_column, end_coord_column, length_column)) \
#         .sort([edge_id_column]) \
#
#
#     graph_time = graph_time_init.map(operations.Week_Day_Hour(enter_time_column, weekday_result_column,
#                                                               hour_result_column)) \
#         .map(operations.Time_diff(enter_time_column, leave_time_column, time_delta_column)) \
#         .map(operations.Project([edge_id_column, time_delta_column, weekday_result_column, hour_result_column])) \
#         .sort([edge_id_column]) \
#         .join(operations.InnerJoiner(), graph_length, [edge_id_column]) \
#         .map(operations.Devide_mapper(length_column, time_delta_column, speed_column)) \
#         .sort([edge_id_column, weekday_result_column, hour_result_column]) \
#         .reduce(operations.CalcMean(speed_column, speed_result_column),
#                 [weekday_result_column, hour_result_column]) \
#         .sort([weekday_result_column, hour_result_column]) \
#
#     return graph_time