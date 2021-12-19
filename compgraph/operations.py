from datetime import datetime
import heapq
import itertools
import math
import operator
import string
import sys
import typing as tp
from abc import abstractmethod, ABC
from math import radians
from math import log, sin, cos, radians, acos

from dateutil import parser as dateutil_parser

TRow = tp.Dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


# Operations


class Mapper(ABC):
    """Base class for mappers"""

    @abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """
        :param row: one table row
        """
        pass


class Map(Operation):
    """Map operation"""

    def __init__(self, mapper: Mapper) -> None:
        self.mapper = mapper

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        """
        :param rows: iterator over TRow
        :return: Generator of mapped TRow
        """
        for row in rows:
            yield from self.mapper(row)


class Reducer(ABC):
    """Base class for reducers"""

    @abstractmethod
    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        pass


class Reduce(Operation):
    """Reduce operation"""

    def __init__(self, reducer: Reducer, keys: tp.Tuple[str, ...]) -> None:
        self.reducer = reducer
        self.keys = keys

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        """
        :param rows: iterator over TRow, sorted by self.keys
        :return: reduced groups of TRow
        """

        def get_value(row: TRow) -> tp.Optional[tp.Iterable[str]]:
            """get values in TRow pointed by key"""
            return [row[key] for key in self.keys]

        for _, g in itertools.groupby(rows, get_value):
            yield from self.reducer(self.keys, g)


class Joiner(ABC):
    """Base class for joiners"""

    def __init__(self, suffix_a: str = "_1", suffix_b: str = "_2") -> None:
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b

    @abstractmethod
    def __call__(
            self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable
    ) -> TRowsGenerator:
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass

    def _inner_join(
            self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable
    ) -> TRowsIterable:
        rows_b_cp = list(rows_b)
        suffix_a, suffix_b = None, None

        for row_a in rows_a:
            for row_b in rows_b_cp:

                if suffix_a is None:
                    suffix_a = {k for k in row_a.keys() if k in row_b and k not in keys}

                if suffix_b is None:
                    suffix_b = {k for k in row_b.keys() if k in row_a and k not in keys}

                res = {
                          k + self._a_suffix if k in suffix_a else k: row_a[k]
                          for k in row_a.keys()
                      } | {
                          k + self._b_suffix if k in suffix_b else k: row_b[k]
                          for k in row_b.keys()
                      }

                yield res


class Join(Operation):
    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self.keys = keys
        self.joiner = joiner

    def __call__(
            self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any
    ) -> TRowsGenerator:
        def grouper(row):  # type: ignore
            return tuple(row[key] for key in self.keys)

        def take_next_from_iterable(iterable_obj: tp.Iterator[tp.Any]) -> tp.Any:
            try:
                return next(iterable_obj)
            except StopIteration:
                return None, None

        rows_a, rows_b = rows, args[0]

        groups_a = itertools.groupby(rows_a, key=grouper)
        groups_b = itertools.groupby(rows_b, key=grouper)

        need_take_a = True
        need_take_b = True

        key_a, group_a = None, None
        key_b, group_b = None, None

        while True:
            if need_take_a:
                key_a, group_a = take_next_from_iterable(groups_a)
            if need_take_b:
                key_b, group_b = take_next_from_iterable(groups_b)

            if group_b is None and group_a is None:
                break

            if key_a == key_b:
                need_take_a = True
                need_take_b = True

                yield from self.joiner(self.keys, group_a or [], group_b or [])
            # elif key_a is None or (key_b is not None and key_b < key_a):
            elif key_a is not None and (key_b is None or key_b >= key_a):
                need_take_a = True
                need_take_b = False
                yield from self.joiner(self.keys, group_a or [], [])
            else:
                need_take_a = False
                need_take_b = True
                yield from self.joiner(self.keys, [], group_b or [])


# Dummy operators

class DummyMapper(Mapper):
    """Yield exactly the row passed"""

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row


class FirstReducer(Reducer):
    """Yield only first row from passed ones"""

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        first_row: tp.Union[TRow, None] = None
        for row in rows:
            if first_row is None:
                first_row = row
        assert first_row is not None
        yield first_row


# Mappers


class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = row[self.column].translate(str.maketrans('', '', string.punctuation))
        yield row


class LowerCase(Mapper):
    """Replace column value with value in lower case"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = row[self.column].lower()
        yield row


class Split(Mapper):
    """Split row on multiple rows by separator"""

    def __init__(self, column: str, separator: tp.Optional[str] = None) -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self.column = column
        self.separator = separator

    def __call__(self, row: TRow) -> TRowsGenerator:
        for part in row[self.column].split(self.separator):
            initial_row = row.copy()
            initial_row[self.column] = part
            yield initial_row


class Product(Mapper):
    """Calculates product of multiple columns"""

    def __init__(self, columns: tp.Sequence[str], result_column: str = 'product') -> None:
        """
        :param columns: column names to product
        :param result_column: column name to save product in
        """
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        res = 1
        for col in self.columns:
            res *= row[col]
        row[self.result_column] = res
        yield row


class Filter(Mapper):
    """Remove records that don't satisfy some condition"""

    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:
        """
        :param condition: if condition is not true - remove record
        """
        self.condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self.condition(row):
            yield row


class Project(Mapper):
    """Leave only mentioned columns"""

    def __init__(self, columns: tp.Sequence[str]) -> None:
        """
        :param columns: names of columns
        """
        self.columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield {key: row[key] for key in self.columns}


class Devide_mapper(Mapper):
    """Divide value from one column to number"""

    def __init__(self, numerator: str, denominator: str, result_column: str) -> None:
        self.numerator = numerator
        self.denominator = denominator
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        new_row = row.copy()
        new_row[self.result_column] = row[self.numerator] / row[self.denominator]
        yield new_row


class Idf(Mapper):
    """Count idf metrics based on number of docs and \
            number of docs containing the word"""

    def __init__(self, doc_count: str, num_word_entries: str, text_column: str, result_column: str) -> None:
        """
        :param doc_count: name of doc_count column
        :param num_word_entries: name of word entries number column
        :param text_column: name of column with word
        :param result_colum: name of column for idf
        """
        self.doc_count = doc_count
        self.num_word_entries = num_word_entries
        self.text_column = text_column
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        total_doc = row[self.doc_count]
        entries_count = row[self.num_word_entries]
        word = row[self.text_column]

        result = dict()
        result[self.text_column] = word
        result[self.result_column] = math.log(total_doc / entries_count)
        yield result


class Pmi(Mapper):
    """Count pmi metrics based on frequency of word \
            in docs and total"""

    def __init__(self, doc_freq: str, total_freq: str, result_column: str) -> None:
        """
        :param doc_freq: name of doc frequency column
        :param total_freq: name of total frequency column
        :param result_colum: name of column for pmi
        """
        self.doc_freq = doc_freq
        self.total_freq = total_freq
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        doc_freq = row[self.doc_freq]
        total_freq = row[self.total_freq]
        row[self.result_column] = math.log(doc_freq / total_freq)
        yield row


class Len_mapper(Mapper):
    """Calc len among two points"""

    def __init__(self, start: str, stop: str, len: str) -> None:
        self.start = start
        self.stop = stop
        self.len = len

    def __call__(self, row: TRow) -> TRowsGenerator:
        row_new = row.copy()
        start_ = row[self.start]
        stop_ = row[self.stop]
        d_ = 6371. * acos(
            sin(radians(start_[1])) * sin(radians(stop_[1])) + cos(radians(start_[1])) * cos(radians(stop_[1]))
            * (cos(radians(stop_[0]) - radians(start_[0]))))
        row_new[self.len] = d_
        yield row_new


class Week_Day_Hour(Mapper):
    """Day of the week and hour from Date"""

    def __init__(self, date_col: str, day_of_week: str, hour_col: str):
        self.date_col = date_col
        self.day_of_week = day_of_week
        self.hour_col = hour_col

    def __call__(self, row: TRow) -> TRowsGenerator:
        row_new = row.copy()
        if "." in row[self.date_col]:
            row_new[self.day_of_week] = datetime.strptime(row[self.date_col], "%Y%m%dT%H%M%S.%f").strftime("%A")[:3]
            row_new[self.hour_col] = datetime.strptime(row[self.date_col], "%Y%m%dT%H%M%S.%f").hour
        else:
            row_new[self.day_of_week] = datetime.strptime(row[self.date_col], "%Y%m%dT%H%M%S").strftime("%A")[:3]
            row_new[self.hour_col] = datetime.strptime(row[self.date_col], "%Y%m%dT%H%M%S").hour

        yield row_new


class Time_diff(Mapper):
    """Calc time-delta between two dates"""

    def __init__(self, start_date: str, end_date: str, time_delta: str):
        self.start_date = start_date
        self.end_date = end_date
        self.time_delta = time_delta

    def __call__(self, row: TRow) -> TRowsGenerator:
        if "." in row[self.start_date]:
            time1 = datetime.strptime(row[self.start_date], "%Y%m%dT%H%M%S.%f")
        else:
            time1 = datetime.strptime(row[self.start_date], "%Y%m%dT%H%M%S")
        if "." in row[self.end_date]:
            time2 = datetime.strptime(row[self.end_date], "%Y%m%dT%H%M%S.%f")
        else:
            time2 = datetime.strptime(row[self.end_date], "%Y%m%dT%H%M%S")

        row_new = row.copy()
        d_ = (time2 - time1).total_seconds() / 3600
        row_new[self.time_delta] = d_
        yield row_new


class ProcessLength(Mapper):
    """Get edge length"""

    def __init__(self, start_coord_column: str, end_coord_column: str, length_column: str) -> None:
        """
        :param start_coord_column: name of start column
        :param end_coord_column: name of end column
        :param length_column: name of column for length
        """
        self.start = start_coord_column
        self.end = end_coord_column
        self.length = length_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        l1, f1 = row[self.start]
        l2, f2 = row[self.end]
        l1 = radians(l1)
        l2 = radians(l2)
        f1 = radians(f1)
        f2 = radians(f2)
        row[self.length] = 6371 * 2 * math.asin(math.sqrt(math.sin(f2 / 2 - f1 / 2) * math.sin(f2 / 2 - f1 / 2) +
                                                          math.cos(f1) * math.cos(f2) * math.sin(l2 / 2 - l1 / 2) *
                                                          math.sin(l2 / 2 - l1 / 2)))
        yield row


class ProcessTime(Mapper):
    """Get edge length"""

    def __init__(self, enter_time_column: str, leave_time_column: str, time_column: str, weekday_column: str,
                 hour_column: str) -> None:
        """
        :param enter_time_column: name of enter time column
        :param leave_time_column: name of leave time column
        :param time_column: name of column for time
        :param weekday_column: name of column for week day
        :param hour_column: name of column for hour
        """
        self.enter = enter_time_column
        self.leave = leave_time_column
        self.time = time_column
        self.day = weekday_column
        self.hour = hour_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        date1 = dateutil_parser.parse(row[self.enter])
        date2 = dateutil_parser.parse(row[self.leave])

        row[self.day] = date1.strftime('%a')
        row[self.hour] = date1.hour
        row[self.time] = (date2 - date1).total_seconds()
        yield row


class ProcessSpeed(Mapper):
    """Get speed based on length and time"""

    def __init__(self, length_column: str, time_column: str, speed_column: str) -> None:
        """
        :param length_column: column for total length
        :param time_column: column for total time
        :param speed_column: column for redult speed
        """
        self.length = length_column
        self.time = time_column
        self.speed = speed_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.speed] = row[self.length] / row[self.time] * 3600
        yield row


# Reducers


class TopN(Reducer):
    """Calculate top N by value"""

    def __init__(self, column: str, n: int) -> None:
        """
        :param column: column name to get top by
        :param n: number of top values to extract
        """
        self.column_max = column
        self.n = n

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        for row in heapq.nlargest(self.n, rows, key=operator.itemgetter(self.column_max)):
            yield row


class TermFrequency(Reducer):
    """Calculate frequency of values in column"""

    def __init__(self, words_column: str, result_column: str = 'tf') -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self.words_column = words_column
        self.result_column = result_column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        counts: tp.Dict[tp.Any, int] = dict()
        length = 0
        row_sample = None
        for row in rows:
            if row_sample is None:
                row_sample = row
            word = row[self.words_column]
            if word not in counts:
                counts[word] = 1
            else:
                counts[word] += 1
            length += 1

        assert row_sample is not None
        for word, count in counts.items():
            result: TRow = dict()
            for key in group_key:
                result[key] = row_sample[key]
            result[self.words_column] = word
            result[self.result_column] = count / length
            yield result


class Count(Reducer):
    """Count rows passed and yield single row as a result"""

    def __init__(self, column: str) -> None:
        """
        :param column: name of column to count
        """
        self.column = column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        result: TRow = dict()
        row_sample = None
        length = 0
        for row in rows:
            if row_sample is None:
                row_sample = row
            length += 1
        assert row_sample is not None
        for key in group_key:
            result[key] = row_sample[key]
        result[self.column] = length
        yield result


class SafeCount(Reducer):
    """Count rows passed and yield multiple row as a result"""

    def __init__(self, column: str) -> None:
        """
        :param column: name of column to count
        """
        self.column = column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        result = dict()
        row_sample = None
        length = 0
        for row in rows:
            if row_sample is None:
                row_sample = row
            length += 1

        assert row_sample is not None
        for key in group_key:
            result[key] = row_sample[key]
        result[self.column] = length
        for i in range(length):
            yield result


class Sum(Reducer):
    """
    Sum values aggregated by key
    Example for key=('a',) and column='b'
        {'a': 1, 'b': 2, 'c': 4}
        {'a': 1, 'b': 3, 'c': 5}
        =>
        {'a': 1, 'b': 5}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for sum column
        """
        self.column = column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        sum_ = 0
        keys: TRow = {}
        print('lolo', self.column, file=sys.stderr)

        for r in rows:
            sum_ += r[self.column]
            if not keys:
                keys = {k: r[k] for k in group_key}

        yield keys | {self.column: sum_}


class MultipleSum(Reducer):
    """Sum values in columns passed and yield single row as a result"""

    def __init__(self, columns: tp.Iterable[str]) -> None:
        """
        :param column: name of columns to sum
        """
        self.columns = columns

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        result = dict()
        row_sample = None
        s = dict()

        for col in self.columns:
            s[col] = 0

        for row in rows:
            if row_sample is None:
                row_sample = row
            for col in self.columns:
                s[col] += row[col]

        assert row_sample is not None
        for key in group_key:
            result[key] = row_sample[key]
        for col in self.columns:
            result[col] = s[col]
        yield result


class CalcMean(Reducer):
    """Calc mean of a group"""

    def __init__(self, column: str, mean_val: str) -> None:
        self.column = column
        self.mean_val = mean_val
        self.sum = 0

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        self.sum = 0
        num_of_rows = 0
        for row in rows:
            num_of_rows += 1
            self.sum += row[self.column]

        row_new = {key: row[key] for key in row if key in group_key}
        value = self.sum / num_of_rows
        row_new[self.mean_val] = value
        yield row_new


# Joiners

class InnerJoiner(Joiner):
    """Join with inner strategy"""

    def __call__(
            self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable
    ) -> TRowsGenerator:
        if rows_a or rows_b:
            yield from self._inner_join(keys, rows_a, rows_b)
        else:
            return


class OuterJoiner(Joiner):
    """Join with outer strategy"""

    def __call__(
            self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable
    ) -> TRowsGenerator:
        rows_b_cp = list(rows_b)
        if not rows_a:
            yield from rows_b_cp
        elif not rows_b_cp:
            yield from rows_a
        else:
            yield from self._inner_join(keys, rows_a, rows_b_cp)


class LeftJoiner(Joiner):
    """Join with left strategy"""

    def __call__(
            self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable
    ) -> TRowsGenerator:
        rows_b_cp = list(rows_b)
        if not rows_a:
            return
        if not rows_b_cp:
            yield from rows_a
        else:
            yield from self._inner_join(keys, rows_a, rows_b_cp)


class RightJoiner(Joiner):
    """Join with right strategy"""

    def __call__(
            self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable
    ) -> TRowsGenerator:
        if not rows_b:
            return
        if not rows_a:
            yield from rows_b
        else:
            yield from self._inner_join(keys, rows_a, rows_b)
