import unittest
from unittest.mock import patch, MagicMock
from migrate_module import create_table, handle_answer_data, insert_data

class DBMigrationTest(unittest.TestCase):

    @patch("migrate_module.create_engine")
    @patch("migrate_module.Table")
    def test_create_table(self, MockTable, MockCreateEngine):
        mock_metadata = MagicMock()
        mock_engine = MagicMock()
        MockCreateEngine.return_value = mock_engine

        create_table("test_table_2", mock_metadata, [], mock_engine)
        mock_metadata.assert_called_once()  # ensure our mock was used

    @patch("migrate_module.select")
    @patch("migrate_module.create_engine")
    @patch("migrate_module.Table")
    def test_handle_answer_data(self, MockTable, MockCreateEngine, MockSelect):
        mock_table = MagicMock()
        MockTable.return_value = mock_table

        mock_engine = MagicMock()
        MockCreateEngine.return_value = mock_engine

        mock_query = MagicMock()
        MockSelect.return_value = mock_query

        handle_answer_data(mock_table, 1, mock_engine, "test_table", mock_engine)
        mock_engine.execute.assert_called_once()

    @patch("migrate_module.select")
    @patch("migrate_module.create_engine")
    @patch("migrate_module.Table")
    def test_insert_data(self, MockTable, MockCreateEngine, MockSelect):
        mock_table = MagicMock()
        MockTable.return_value = mock_table

        mock_engine = MagicMock()
        MockCreateEngine.return_value = mock_engine

        mock_query = MagicMock()
        MockSelect.return_value = mock_query


        insert_data(mock_engine, mock_table, "test_table", 1, mock_engine)
        mock_engine.execute.assert_called()

if __name__ == "__main__":
    unittest.main()
