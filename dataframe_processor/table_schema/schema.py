from typing import List

from django.utils.translation import gettext as _


class ValidationError(Exception):

    def __init__(self, message: str, column=None, field_value=None):
        self.message = message
        self.column = column
        self.field_value = field_value

    def __str__(self):
        if self.column:
            return _(f'{self.column} - {self.message}')
        return _(f'Error: {self.message}')


class Validator(object):

    def __init__(self, validation_function, msg: str = None):
        self.validation_function = validation_function
        self.msg = msg or _('Invalid value')


class Column(object):

    def __init__(self, name, label, ctype, required=True, validators: List[Validator] = None):
        if validators is None:
            validators = []

        self.name = name
        self.label = label
        self.ctype = ctype
        self.required = required
        self.validators: List[Validator] = validators


class TableSchema(object):

    def __init__(self, columns: List[Column]):
        self.columns = columns

    def validate_missing_columns(self, df):
        missing_columns = []
        for column in self.columns:
            if column.label not in df.columns:
                missing_columns.append(column.label)
        if missing_columns:
            raise ValidationError(_(f'Missing columns: {missing_columns}'))

    def rename_columns(self, df):
        return df.rename(columns={
            column.label: column.name for column in self.columns
        })

    def validate_row(self, row) -> List[ValidationError]:
        errors = []
        for column in self.columns:
            value = row.get(column.name)
            if not value:
                if column.required:
                    errors.append(ValidationError(_('Required value'), column=column.label))
            elif not type(value) is column.ctype:
                errors.append(
                    ValidationError(
                        _(f'Column {column.name} is of incorrect type: {type(value).__name__}, it should be {column.ctype.__name__}'),
                        column=column.label))
            else:
                for validator in column.validators:
                    if not validator.validation_function(value):
                        errors.append(ValidationError(validator.msg, column=column.label, field_value=value))
        return errors
