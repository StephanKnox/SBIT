import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("SbitUnitTests")
        .master("local[*]")
        .config("spark.driver.host", "localhost")           # MAC OS specific
        .config("spark.driver.bindAddress", "127.0.0.1")    # MAC OS specific
        .config("spark.local.ip", "127.0.0.1")              # MAC OS specific
        .config("spark.ui.enabled", "false")                # for CI pipeline Agent
        .config("spark.driver.memory", "2g")                # for CI pipeline Agent
        .config("spark.sql.shuffle.partitions", "4")        # for CI pipeline Agent
        .getOrCreate()
        )   
    yield spark

    # Cleanup after all tests
    spark.stop()