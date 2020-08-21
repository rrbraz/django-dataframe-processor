from datetime import date
from typing import List

from django.utils.translation import gettext as _

from dataframe_processor.table_schema.schema import Validator


def date_range_validator(begin: date, end: date):
    return Validator(lambda s: begin < s < end, msg=_('Invalid date range'))


def choices_validator(values_list: List[str]):
    return Validator(lambda s: s in values_list, msg=_(f'Value not in : {list}'))
