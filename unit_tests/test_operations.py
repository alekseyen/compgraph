from compgraph import operations as ops


def test_length() -> None:
    tests: ops.TRowsIterable = [
        {'start': [37.84870228730142, 55.73853974696249], 'end': [37.8490418381989, 55.73832445777953],
         'edge_id': 8414926848168493057},
        {'start': [37.524768467992544, 55.88785375468433], 'end': [37.52415172755718, 55.88807155843824],
         'edge_id': 5342768494149337085}
    ]
    etalon: ops.TRowsIterable = [
        {'start': [37.84870228730142, 55.73853974696249], 'end': [37.8490418381989, 55.73832445777953],
         'edge_id': 8414926848168493057, 'length': 0.03201389419178626},
        {'start': [37.524768467992544, 55.88785375468433], 'end': [37.52415172755718, 55.88807155843824],
         'edge_id': 5342768494149337085, 'length': 0.04544992068115006}
    ]

    result = ops.Map(ops.Len_mapper(start='start', stop='end', len='length'))(tests)
    assert etalon == list(result)


def test_week_day_hour() -> None:
    tests: ops.TRowsIterable = [
        {'leave_time': '20171020T112238.723000', 'enter_time': '20171020T112237.427000',
         'edge_id': 8414926848168493057},
        {'leave_time': '20171011T145553.040000', 'enter_time': '20171011T145551.957000',
         'edge_id': 8414926848168493057}
    ]
    etalon: ops.TRowsIterable = [
        {'leave_time': '20171020T112238.723000', 'enter_time': '20171020T112237.427000',
         'edge_id': 8414926848168493057, 'hour': 11, 'weekday': 'Fri'},
        {'leave_time': '20171011T145553.040000', 'enter_time': '20171011T145551.957000',
         'edge_id': 8414926848168493057, 'hour': 14, 'weekday': 'Wed'}
    ]

    result = ops.Map(ops.Week_Day_Hour(date_col='enter_time',
                                       day_of_week='weekday',
                                       hour_col='hour'))(tests)
    assert etalon == list(result)


def test_time_diff() -> None:
    tests: ops.TRowsIterable = [
        {'leave_time': '20171020T112238.723000', 'enter_time': '20171020T112237.427000',
         'edge_id': 8414926848168493057},
        {'leave_time': '20171011T145553.040000', 'enter_time': '20171011T145551.957000',
         'edge_id': 8414926848168493057}
    ]
    etalon: ops.TRowsIterable = [
        {'leave_time': '20171020T112238.723000', 'enter_time': '20171020T112237.427000',
         'edge_id': 8414926848168493057, 'time': 0.00036},
        {'leave_time': '20171011T145553.040000', 'enter_time': '20171011T145551.957000',
         'edge_id': 8414926848168493057, 'time': 0.00030083333333333335}
    ]

    result = ops.Map(ops.Time_diff(start_date='enter_time',
                                   end_date='leave_time',
                                   time_delta='time'))(tests)
    assert etalon == list(result)


def test_delete_short_words() -> None:
    tests: ops.TRowsIterable = [
        {'player_id': 0, 'username': 'root'},
        {'player_id': 1, 'username': 'Xer'},
        {'player_id': 2, 'username': 'jas'}
    ]
    etalon: ops.TRowsIterable = [
        {'player_id': 0, 'username': 'root'}
    ]


def test_division() -> None:
    tests: ops.TRowsIterable = [
        {'num': 1, 'denom': 2},
        {'num': 2, 'denom': 2},
        {'num': 3, 'denom': 3}
    ]

    etalon: ops.TRowsIterable = [
        {'num': 1, 'denom': 2, 'res': 0.5},
        {'num': 2, 'denom': 2, 'res': 1.0},
        {'num': 3, 'denom': 3, 'res': 1.0}
    ]

    result = ops.Map(ops.Devide_mapper('num', 'denom', 'res'))(tests)
    assert etalon == list(result)


def test_delete_columns() -> None:
    tests: ops.TRowsIterable = [
        {'test_id': 1, 'axis': 'x', 'value': 2},
        {'test_id': 2, 'axis': 'y', 'value': 1},
        {'test_id': 3, 'axis': 'z', 'value': 3}
    ]

    etalon: ops.TRowsIterable = [
        {'value': 2},
        {'value': 1},
        {'value': 3}
    ]


def test_calc_mean() -> None:
    tests: ops.TRowsIterable = [
        {'test_id': 1, 'axis': 'a', 'value': 2},
        {'test_id': 2, 'axis': 'b', 'value': 1},
        {'test_id': 3, 'axis': 'c', 'value': 6}
    ]

    etalon: ops.TRowsIterable = [{'mean': 3.0}]
    result = ops.Reduce(ops.CalcMean(column='value', mean_val='mean'), ())(tests)

    assert etalon == list(result)
