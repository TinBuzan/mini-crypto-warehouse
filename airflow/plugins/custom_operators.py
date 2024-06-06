from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Placeholder File

class CustomDbtRunOperator(BaseOperator):
    @apply_defaults
    def __init__(self, dbt_command, profiles_dir, dir, *args, **kwargs):
        super(CustomDbtRunOperator, self).__init__(*args, **kwargs)
        self.dbt_command = dbt_command
        self.profiles_dir = profiles_dir
        self.dir = dir

    def execute(self, context):
        pass