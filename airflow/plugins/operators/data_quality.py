from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_id = "",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_id = redshift_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_id)
        for t in self.tables:
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(t))
            if len(records)<1:
                self.log.info("{} returned no results".format(t))
                raise ValueError("Data quality check failed: No results returned from {}".format(t))

        self.log.info("Data Quality Check successful for table {}".format(t))
                
        