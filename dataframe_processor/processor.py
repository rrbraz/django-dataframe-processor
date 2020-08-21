import numpy as np
import pandas as pd
from django.db import transaction
from django.utils.translation import gettext as _

from .table_schema.schema import TableSchema

IMPORT_STATUS = _('Import Status')


class DataframeProcessor(object):
    schema: TableSchema = None

    class ProcessException(Exception):
        pass

    def __init__(self, df: pd.DataFrame):
        self.error = False
        self.df = df
        self.status = ''
        self.report = []
        self._clean_df()

    def _clean_df(self) -> None:
        if self.schema:
            self.schema.validate_missing_columns(self.df)
            self.df = self.schema.rename_columns(self.df)

        # trims white spaces
        self.df = self.df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # replaces NaNs with None
        self.df = self.df.replace({np.nan: None})

    def clean_row(self, row):
        if self.schema:
            errors = self.schema.validate_row(row)
            if errors:
                raise DataframeProcessor.ProcessException(*[str(e) for e in errors])
        return row

    def save(self, data) -> str:
        return 'OK'

    def process_row(self, row):
        cleaned = self.clean_row(row)
        return self.save(cleaned)

    def finish_processing(self):
        pass

    @transaction.atomic
    def process(self) -> bool:

        self.df[IMPORT_STATUS] = ''
        i = -1
        try:
            for i, row in self.df.iterrows():
                try:
                    message = self.process_row(row)
                    self.report.append({'line': i + 1, 'status': message})
                except DataframeProcessor.ProcessException as e:
                    self.error = True
                    self.report.append({'line': i + 1, 'status': e.args})
                    message = str(e)
                self.df.at[i, IMPORT_STATUS] = message
        except Exception as e:
            try:  # tries to capture exception with sentry if it's installed
                import sentry_sdk
                sentry_sdk.capture_exception(e)
            except ImportError:
                pass
            self.error = True
            self.status = str(e)
            self.report.append({'line': i + 1, 'status': self.status})

        if not self.error:
            self.finish_processing()

        if self.error:
            transaction.set_rollback(True)
            return False
        return True
