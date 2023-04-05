from __future__ import annotations

import csv
import json
import tempfile
import uuid

import numpy as np
import pyarrow as pa
import pyarrow.parquet as papq
import pytest

from daft.api_annotations import APITypeError
from daft.dataframe import DataFrame
from daft.datatype import DataType


class MyObj:
    pass


class MyObj2:
    pass


COL_NAMES = [
    "sepal_length",
    "sepal_width",
    "petal_length",
    "petal_width",
    "variety",
]


@pytest.mark.parametrize("read_method", ["read_csv", "read_json", "read_parquet"])
def test_load_missing(read_method):
    """Loading data from a missing filepath"""
    with pytest.raises(FileNotFoundError):
        getattr(DataFrame, read_method)(str(uuid.uuid4()))


@pytest.mark.parametrize("data", [{"foo": [1, 2, 3]}, [{"foo": i} for i in range(3)], "foo"])
def test_error_thrown_create_dataframe_constructor(data) -> None:
    with pytest.raises(ValueError):
        DataFrame(data)


def test_wrong_input_type():
    with pytest.raises(APITypeError):
        DataFrame.from_pydict("invalid input")


###
# List tests
###


def test_create_dataframe_list(valid_data: list[dict[str, float]]) -> None:
    df = DataFrame.from_pylist(valid_data)
    assert set(df.column_names) == set(COL_NAMES)


def test_create_dataframe_list_empty() -> None:
    df = DataFrame.from_pylist([])
    assert df.column_names == []


def test_create_dataframe_list_ragged_keys() -> None:
    df = DataFrame.from_pylist(
        [
            {"foo": 1},
            {"foo": 2, "bar": 1},
            {"foo": 3, "bar": 2, "baz": 1},
        ]
    )
    assert df.to_pydict() == {
        "foo": [1, 2, 3],
        "bar": [None, 1, 2],
        "baz": [None, None, 1],
    }


def test_create_dataframe_list_empty_dicts() -> None:
    df = DataFrame.from_pylist([{}, {}, {}])
    assert df.column_names == []


def test_create_dataframe_list_non_dicts() -> None:
    with pytest.raises(ValueError) as e:
        DataFrame.from_pylist([1, 2, 3])
    assert "Expected list of dictionaries of {column_name: value}" in str(e.value)


###
# Dict tests
###


def test_create_dataframe_pydict(valid_data: list[dict[str, float]]) -> None:
    pydict = {k: [item[k] for item in valid_data] for k in valid_data[0].keys()}
    df = DataFrame.from_pydict(pydict)
    assert set(df.column_names) == set(COL_NAMES)


def test_create_dataframe_empty_pydict() -> None:
    df = DataFrame.from_pydict({})
    assert df.column_names == []


def test_create_dataframe_pydict_ragged_col_lens() -> None:
    with pytest.raises(ValueError) as e:
        DataFrame.from_pydict({"foo": [1, 2], "bar": [1, 2, 3]})
    assert "Expected all columns to be of the same length" in str(e.value)


def test_create_dataframe_pydict_bad_columns() -> None:
    with pytest.raises(ValueError) as e:
        DataFrame.from_pydict({"foo": "somestring"})
    assert "Creating a Series from data of type" in str(e.value)


@pytest.mark.parametrize(
    ["data", "expected_dtype"],
    [
        pytest.param([None, 2, 3], DataType.int64(), id="arrow_int"),
        pytest.param([None, 1.0, 3.0], DataType.float64(), id="arrow_float"),
        pytest.param([None, 1.0, 3], DataType.float64(), id="arrow_mixed_numbers"),
        pytest.param([None, "b", "c"], DataType.string(), id="arrow_str"),
        pytest.param([None, None, None], DataType.null(), id="arrow_nulls"),
        pytest.param(np.array([1, 2, 3], dtype=np.int64), DataType.int64(), id="np_int"),
        pytest.param(np.array([None, "foo", "bar"], dtype=np.object_), DataType.string(), id="np_string"),
        pytest.param(pa.array([1, 2, 3]), DataType.int64(), id="pa_int"),
        pytest.param(pa.chunked_array([pa.array([1, 2, 3])]), DataType.int64(), id="pa_int_chunked"),
        # TODO: [TPCH][PY] activate tests when Python objects are implemented
        # pytest.param([None, MyObj(), MyObj()], DataType.python(MyObj), id="py_objs"),
        # pytest.param([None, MyObj(), MyObj2()], DataType.python_object(), id="heterogenous_py_objs"),
        # pytest.param(np.array([None, MyObj(), MyObj()], dtype=np.object_), DataType.python(MyObj), id="np_object"),
        # TODO: [TPCH][NESTED] activate tests when Nested types are implemented
        # pytest.param([None, {"foo": 1}, {"bar": 1}], DataType.python(dict), id="arrow_struct"),
        # pytest.param([np.array([1]), np.array([2]), np.array([3])], DataType.python(np.ndarray), id="numpy_arrays"),
        # pytest.param(pa.array([[1, 2, 3], [1, 2], [1]]), DataType.python(list), id="pa_nested"),
        # pytest.param(pa.chunked_array([pa.array([[1, 2, 3], [1, 2], [1]])]), DataType.python(list), id="pa_nested_chunked"),
        # pytest.param(np.ones((3, 3)), DataType.python(np.ndarray), id="np_nested"),
    ],
)
def test_load_pydict_types(data, expected_dtype):
    data_dict = {"x": data}
    daft_df = DataFrame.from_pydict(data_dict)
    daft_df.collect()
    collected_data = daft_df.to_pydict()

    assert list(collected_data.keys()) == ["x"]
    assert daft_df.schema()["x"].dtype == expected_dtype


###
# CSV tests
###


def test_create_dataframe_csv(valid_data: list[dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        header = list(valid_data[0].keys())
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows([[item[col] for col in header] for item in valid_data])
        f.flush()

        df = DataFrame.read_csv(f.name)
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


@pytest.mark.parametrize("has_headers", [True, False])
def test_create_dataframe_csv_provide_headers(valid_data: list[dict[str, float]], has_headers: bool) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        header = list(valid_data[0].keys())
        writer = csv.writer(f)
        if has_headers:
            writer.writerow(header)
        writer.writerows([[item[col] for col in header] for item in valid_data])
        f.flush()

        cnames = [f"foo{i}" for i in range(5)]
        df = DataFrame.read_csv(f.name, has_headers=has_headers, column_names=cnames)
        assert df.column_names == cnames

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == cnames
        assert len(pd_df) == len(valid_data)


def test_create_dataframe_csv_generate_headers(valid_data: list[dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        header = list(valid_data[0].keys())
        writer = csv.writer(f)
        writer.writerows([[item[col] for col in header] for item in valid_data])
        f.flush()

        cnames = [f"f{i}" for i in range(5)]
        df = DataFrame.read_csv(f.name, has_headers=False)
        assert df.column_names == cnames

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == cnames
        assert len(pd_df) == len(valid_data)


def test_create_dataframe_csv_column_projection(valid_data: list[dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        header = list(valid_data[0].keys())
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows([[item[col] for col in header] for item in valid_data])
        f.flush()

        col_subset = COL_NAMES[:3]

        df = DataFrame.read_csv(f.name)
        df = df.select(*col_subset)
        assert df.column_names == col_subset

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == col_subset
        assert len(pd_df) == len(valid_data)


def test_create_dataframe_csv_custom_delimiter(valid_data: list[dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        header = list(valid_data[0].keys())
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(header)
        writer.writerows([[item[col] for col in header] for item in valid_data])
        f.flush()

        df = DataFrame.read_csv(f.name, delimiter="\t")
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


###
# JSON tests
###


def test_create_dataframe_json(valid_data: list[dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        for data in valid_data:
            f.write(json.dumps(data))
            f.write("\n")
        f.flush()

        df = DataFrame.read_json(f.name)
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


def test_create_dataframe_json_column_projection(valid_data: list[dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        for data in valid_data:
            f.write(json.dumps(data))
            f.write("\n")
        f.flush()

        col_subset = COL_NAMES[:3]

        df = DataFrame.read_json(f.name)
        df = df.select(*col_subset)
        assert df.column_names == col_subset

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == col_subset
        assert len(pd_df) == len(valid_data)


@pytest.mark.skip(reason="[RUST-INT][NESTED] Requires list[int64] type")
def test_create_dataframe_json_https() -> None:
    df = DataFrame.read_json("https://github.com/Eventual-Inc/mnist-json/raw/master/mnist_handwritten_test.json.gz")
    df.collect()
    assert set(df.column_names) == {"label", "image"}
    assert len(df) == 10000


###
# Parquet tests
###


def test_create_dataframe_parquet(valid_data: list[dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        table = pa.Table.from_pydict({col: [d[col] for d in valid_data] for col in COL_NAMES})
        papq.write_table(table, f.name)
        f.flush()

        df = DataFrame.read_parquet(f.name)
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


def test_create_dataframe_parquet_column_projection(valid_data: list[dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        table = pa.Table.from_pydict({col: [d[col] for d in valid_data] for col in COL_NAMES})
        papq.write_table(table, f.name)
        f.flush()

        col_subset = COL_NAMES[:3]

        df = DataFrame.read_parquet(f.name)
        df = df.select(*col_subset)
        assert df.column_names == col_subset

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == col_subset
        assert len(pd_df) == len(valid_data)