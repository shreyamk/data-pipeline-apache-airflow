from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql = """
        INSERT INTO {}
        {}
        ;
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_id = "",
                 schema = "",
                 query = "",
                 append_only = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Mapping parameters:
        self.redshift_id = redshift_id
        self.schema = schema
        self.query = query
        self.append_only = append_only

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_id)
        
        if not self.append_only:
            self.log.info("Deleting {} table".format(self.schema))
            redshift_hook.run("DELETE FROM {}".format(self.schema))
        
 
        self.log.info("Inserting data into {} fact table".format(self.schema))
        
        formatted_query = LoadFactOperator.insert_sql.format(
            self.schema,
            self.query
        )
        redshift_hook.run(formatted_query)
       
        
