from datetime import datetime
from unittest import TestCase, mock
from ..historian import Historian, Tag, TagReading


class TestHistorian(TestCase):
    def test_historian_no_connection(self):
        sut = Historian('', '', '', '')
        with self.assertRaises(Exception):
            sut.get_all_tags()

    @mock.patch('pymssql.connect')
    def test_historian_returns_tags(self, mock_connect):
        mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchall \
            .return_value = [{"Id": "test1", "Description": "Test Description", "Units": "gal"}]
        sut = Historian('', '', '', '')
        result = sut.get_all_tags()
        assert len(result) == 1
        self.assertIsInstance(result[0], Tag)

    @mock.patch('pymssql.connect')
    def test_returns_current_value(self, mock_connect):
        mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchall \
            .return_value = [{"Id": "test1", "Timestamp": datetime.now(), "Value": 1.0, "Quality": 194}]
        sut = Historian('', '', '', '')
        result = sut.get_current_tag_reading('test1')
        self.assertIsInstance(result, TagReading)
        self.assertAlmostEqual(result.value, 1.0, 2)

    @mock.patch('pymssql.connect')
    def test_handles_no_current_value(self, mock_connect):
        mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchall \
            .return_value = []
        sut = Historian('', '', '', '')
        result = sut.get_current_tag_reading('test1')
        assert not result
