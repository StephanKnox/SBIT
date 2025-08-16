import pytest
from unittest.mock import MagicMock, mock_open, patch
from pyspark.sql.utils import AnalysisException
from my_sbit_project.utils.common import DatabricksWorkflow

# Dummy concrete class to allow instantiation
class DummyWorkflow(DatabricksWorkflow):
    def launch(self):
        pass

    def get_spark_session(self):
        return MagicMock()
    

@pytest.fixture
def init_args():
    return "LOCAL", "/path_to/fake.yaml"


@patch("builtins.open", new_callable=mock_open, read_data="""
name: Test App
features:
  - login
  - dashboard
""")
def test_read_yaml_WhenFileExists_ReturnsItContent(mock_file, init_args):
        wkf = DummyWorkflow(env=init_args[0], app_cgf=init_args[1])

        mock_file.assert_called_once_with("/path_to/fake.yaml")
        assert wkf.app_cfg == {'name': 'Test App', 'features': ['login', 'dashboard']}


def test_read_yaml_WhenFileNotExists_RaisesFileNotFound(init_args):
    with pytest.raises(FileNotFoundError) as excinfo:
        wkf = DummyWorkflow(env=init_args[0], app_cgf=init_args[1])


def test_exec_sql_WhenValidQuery_SqlIsExecuted(init_args):
    with patch.object(DatabricksWorkflow, "read_yaml", return_value={"key": "value"}):
        wkf = DummyWorkflow(env=init_args[0], app_cgf=init_args[1])
        wkf.spark.sql = MagicMock()
        
        wkf.exec_sql("SELECT * FROM table", {"param": 1})

        wkf.spark.sql.assert_called_once_with("SELECT * FROM table", args={"param": 1})


def test_exec_sql_WhenNotValidQuery_ThrowsAnalysisExcepton(init_args):
    original_exception = AnalysisException("Table not found")

    with patch.object(DatabricksWorkflow, "read_yaml", return_value={"key": "value"}):
        wkf = DummyWorkflow(env=init_args[0], app_cgf=init_args[1])
        wkf.spark.sql = MagicMock()
        wkf.spark.sql.side_effect = original_exception
        
        with pytest.raises(AnalysisException) as excinfo:
            wkf.exec_sql("SELECT * FROM non_existing_table")
            