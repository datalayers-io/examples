import datetime

import pandas
import pyarrow as pa


def print_affected_rows(df: pandas.DataFrame) -> int:
    print("Affected rows: {}".format(df["affected_rows"][0]))


def make_insert_binding() -> pa.RecordBatch:
    tzinfo = datetime.timezone(datetime.timedelta(hours=8))
    ts_data = [
        datetime.datetime(2024, 9, 2, 10, 0, tzinfo=tzinfo),
        datetime.datetime(2024, 9, 2, 10, 5, tzinfo=tzinfo),
        datetime.datetime(2024, 9, 2, 10, 10, tzinfo=tzinfo),
        datetime.datetime(2024, 9, 2, 10, 15, tzinfo=tzinfo),
        datetime.datetime(2024, 9, 2, 10, 20, tzinfo=tzinfo),
    ]
    sid_data = [1, 2, 3, 4, 5]
    value_data = [12.5, 15.3, 9.8, 22.1, 30.0]
    flag_data = [0, 1, 0, 1, 0]

    ts_column = pa.array(ts_data, type=pa.timestamp("ms"))
    sid_column = pa.array(sid_data, type=pa.int32())
    value_column = pa.array(value_data, type=pa.float32())
    flag_column = pa.array(flag_data, type=pa.int8())

    batch = pa.RecordBatch.from_arrays(
        [ts_column, sid_column, value_column, flag_column],
        ["ts", "sid", "value", "flag"],
    )
    return batch


def make_query_binding(sid: int) -> pa.RecordBatch:
    sid_values = pa.array([sid], type=pa.int32())
    binding = pa.RecordBatch.from_arrays([sid_values], ["sid"])
    return binding
