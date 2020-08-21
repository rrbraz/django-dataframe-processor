from unittest.case import TestCase
from unittest.mock import patch

import pandas as pd
from dateutil.relativedelta import relativedelta
from django.utils import timezone

from dataframe_processor.processor import DataframeProcessor
from dataframe_processor.table_schema.schema import TableSchema, Column, ValidationError
from dataframe_processor.table_schema.validators import date_range_validator, choices_validator


class FileProcessorTestCase(TestCase):

    def test_basic_file(self):
        df = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
        processor = DataframeProcessor(df)
        r = processor.process()
        self.assertTrue(r)

    @patch('dataframe_processor.processor.DataframeProcessor.process_row')
    def test_process_exception(self, mock):
        mock.side_effect = DataframeProcessor.ProcessException()
        df = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
        processor = DataframeProcessor(df)
        r = processor.process()
        self.assertFalse(r)

    @patch('dataframe_processor.processor.DataframeProcessor.process_row')
    def test_fatal_exception(self, mock):
        mock.side_effect = Exception()
        df = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
        processor = DataframeProcessor(df)
        r = processor.process()
        self.assertFalse(r)


class FileProcessorWithSchemaTestCase(TestCase):
    class DummyProcessor(DataframeProcessor):
        today = timezone.now().date()
        schema = TableSchema([
            Column('name', label='Name*', ctype=str),
            Column('document_number', label='Document*', ctype=int),
            Column('birth_date', label='Date of birth*', ctype=pd.Timestamp, validators=[
                date_range_validator(begin=today - relativedelta(years=120), end=today),
            ]),
            Column('gender', label='Gender*', ctype=str, validators=[choices_validator(['M', 'F'])], required=False),
        ])

    def test_success(self):
        df = pd.DataFrame([
            ['John Doe', 123456, pd.Timestamp(1960, 6, 8), 'M'],
            ['Jane Doe', 654321, pd.Timestamp(1965, 2, 17), 'F'],
            ['Sydney Doe', 654321, pd.Timestamp(1965, 2, 17), None],
        ], columns=['Name*', 'Document*', 'Date of birth*', 'Gender*'])
        processor = self.DummyProcessor(df)
        r = processor.process()
        self.assertTrue(r, processor.report)

    def test_validations(self):
        df = pd.DataFrame([
            ['John Doe', '123456', pd.Timestamp(1960, 6, 8), 'M'],
            ['Jane Doe', 654321, pd.Timestamp(1965, 2, 17), 'F'],
            ['Sydney Doe', None, pd.Timestamp(1965, 2, 17), 'G'],
        ], columns=['Name*', 'Document*', 'Date of birth*', 'Gender*'])
        processor = self.DummyProcessor(df)
        r = processor.process()
        self.assertFalse(r, processor.report)

    def test_missing_column(self):
        df = pd.DataFrame([
            ['John Doe', 123456, 'M'],
            ['Jane Doe', 654321, 'F'],
            ['Sydney Doe', None, None],
        ], columns=['Name*', 'Document*', 'Gender*'])
        with self.assertRaises(ValidationError):
            self.DummyProcessor(df)
