# Django Dataframe Processor
A django library for importing/processing files or other data that is read as a DataFrame

## Setup
```bash
pip install django-dataframe-processor
```

Add `dataframe_processor` to your Django settings:

```python
INSTALLED_APPS = (
    ...
    'dataframe_processor',
    ...
)
```

## Usage
Create a class extending from DataframeProcessor
```python
from dataframe_processor.processor import DataframeProcessor
from dataframe_processor.table_schema.schema import TableSchema, Column
from dataframe_processor.table_schema.validators import date_range_validator, choices_validator
from dateutil.relativedelta import relativedelta
from django.utils import timezone

class MyFileProcessor(DataframeProcessor):
    today = timezone.now().date()
    schema = TableSchema([
        Column('name', label='Name*', ctype=str),
        Column('document_number', label='Document*', ctype=int),
        Column('birth_date', label='Date of birth*', ctype=pd.Timestamp, validators=[
            date_range_validator(begin=today - relativedelta(years=120), end=today),
        ]),
        Column('gender', label='Gender*', ctype=str, validators=[choices_validator(['M', 'F'])], required=False),
    ])
    
    def clean_row(self, row):
        row = super().clean_row(row)
        # make any aditional validation that you may need here
        # if anything goes wrong you may raise a FileProcessor.ProcessException
        return row

    def save(self, data):
        # save your data to the database or do any other processing you may need
        return 'Ok'
```

Then you can process your files:
```python
import pandas as pd
import MyFileProcessor

df = pd.read_csv('mydata.csv')
processor = MyFileProcessor(df)
success = processor.process()

# the processor generates a report for each line
print(processor.report)
```
