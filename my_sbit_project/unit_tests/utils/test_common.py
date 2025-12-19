import pytest
import yaml
import datetime
from argparse import Namespace
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession, functions as fn
from my_sbit_project.utils.common import ConfigLoader, SqlExecutor, parse_wkf_args, get_dates_from_df, remove_duplicates

"""
Chat Gpt answer: 

https://chatgpt.com/c/692f4591-0a50-832a-80ff-3669a1b0d449
"""

#@pytest.fixture(scope="session")
#def spark():
#    return SparkSession.builder \
#        .master("local[*]") \
#        .appName("unit-tests") \
#        .getOrCreate()

##@pytest.fixture(scope="session")
##def spark():
##    spark = (
##        SparkSession.builder
##        .appName("SbitUnitTests")
##        .master("local[*]")
##        .config("spark.driver.host", "localhost")
##       .config("spark.driver.bindAddress", "127.0.0.1")
##        .config("spark.local.ip", "127.0.0.1")
##        .getOrCreate()
##        )   
##    return spark


def test_read_config_WithValidYaml_ReturnsParsedContent(mocker):
    """
    https://docs.python.org/3/library/unittest.mock.html#patch

    https://docs.python.org/3/library/unittest.mock.html#mock-open
    """
    fake_yaml = "key: value"
    mk_open = mocker.mock_open(read_data = fake_yaml)
    mocker.patch("my_sbit_project.utils.common.open", mk_open)
    mocker.patch("my_sbit_project.utils.common.yaml.safe_load", return_value={"key": "value"})

    result = ConfigLoader._read_yaml("dummy/path.yaml")

    assert result == {"key": "value"}
    yaml.safe_load.assert_called_once()

def test_read_config_NoYamlFile_RaisesException(mocker):
    # 1. Mock open() to raise FileNotFoundError
    mocker.patch("my_sbit_project.utils.common.open", side_effect=FileNotFoundError)

    # 2. Call code and assert that the exception is re-raised
    with pytest.raises(FileNotFoundError):
        ConfigLoader._read_yaml("missing.yaml")

def test_run_WithValidSQL_ReturnsResult(mocker):
    mock_spark = mocker.Mock()
    mock_result = mocker.Mock()
    mock_spark.sql.return_value = mock_result
    executor = SqlExecutor(mock_spark)

    result = executor.run("SELECT 1", in_args={"foo": "bar"})

    assert result == mock_result
    mock_spark.sql.assert_called_once_with("SELECT 1", args={"foo": "bar"})

def test_run_WithVBadSQL_RaisesException(mocker):
    mock_spark = mocker.Mock()
    mock_spark.sql.side_effect = AnalysisException("bad sql")
    executor = SqlExecutor(mock_spark)

    with pytest.raises(AnalysisException):
        executor.run("BROKEN SQL")

def test_parse_wkf_args_GivenNamesspace_ReturnsDict():
    test_namespace = Namespace(foo=1, bar="hello", debug=True)
    
    result = parse_wkf_args(test_namespace)

    assert isinstance(result, dict)
    assert result is not vars(test_namespace)
    assert result == {"foo": 1, "bar": "hello", "debug": True}


def test_get_dates_from_df_GivenDateRange_ReturnsListofDates(spark):
    test_df = spark.createDataFrame(
        [(1, "2025-12-01"),
         (2, "2025-12-04"),
         (3, "2025-12-10")],
        ["col_id", "col_date"]
    ).withColumn("col_date", fn.to_date("col_date"))
    
    result = get_dates_from_df(test_df, "col_date", "2025-11-15", "2025-12-15")
    
    assert result == [datetime.date(2025, 12, 1), datetime.date(2025, 12, 4), datetime.date(2025, 12, 10)]

def test_get_dates_from_df_GivenOneDate_ReturnsListOfOne(spark):
    test_df = spark.createDataFrame(
        [(1, "2025-12-01"),
         (2, "2025-12-04"),
         (3, "2025-12-10")],
        ["col_id", "col_date"]
    ).withColumn("col_date", fn.to_date("col_date"))
    
    result = get_dates_from_df(test_df, "col_date", "2025-12-01", "2025-12-01")
    
    assert result == [datetime.date(2025, 12, 1)]

def test_remove_duplicates_OrderingIsString_ReturnsDedupedOrderedByString(spark):
    test_df = spark.createDataFrame(
        [(1, "abc", "2025-12-01"),
         (1, "abc", "2025-12-04")],
        ["col_id", "col_text", "col_date"]).withColumn("col_date", fn.to_date("col_date"))
    
    result = remove_duplicates(test_df, ["col_id", "col_text"], "col_date")

    assert result.count() == 1
    assert result.collect()[0]["col_date"] == datetime.date(2025, 12, 4)

def test_remove_duplicates_OrderingIsDict_ReturnsDedupedOrderedByDict(spark):
    test_df = spark.createDataFrame(
        [(1, "abc", "2025-12-01", 2),
         (1, "abc", "2025-12-04", 1)],
        ["col_id", "col_text", "col_date", "col_id_2"]).withColumn("col_date", fn.to_date("col_date"))
    
    result = remove_duplicates(test_df, ["col_id", "col_text"], {"col_id_2": "desc", "col_date": "asc"})

    assert result.count() == 1
    assert result.collect()[0]["col_date"] == datetime.date(2025, 12, 1)