
from lib2to3.pytree import Base
from xml.sax.handler import property_declaration_handler
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from sqlalchemy import table


class DBBuilderOperator(BaseOperator):

    ui_color = '#E1A5F7'

    @apply_defaults
    def __init__(self, redshift_conn_id, queries_to_exec=[], *args, **kwargs) -> None:
        super(DBBuilderOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.queries = queries_to_exec
    

    def execute(self, context):
        self.log.info("Database builder running to create tables.")

        if not self.areQueriesAvailable():
            self.log.info("No queries present to execute. Exiting builder...")
            return

        redshift = PostgresHook(self.redshift_conn_id)
        conn = redshift.get_conn()
        cursor = conn.cursor()

        for query_map in self.queries:
            table_name = query_map.get('table_name')
            query = query_map.get('sql_query')
            try:
                self.log.info("Creating {} table in redshift database".format(table_name))
                cursor.execute(query=query)
            except Exception as ex:
                self.log.info("Something went wrong creating table {}. \n Error description:\n{}".format(table_name, ex))
                cursor.close()
                raise Exception("All queries must run correctly to proceed with operator. Aborting, please review code and connections.")

        cursor.close()
        conn.commit()
        self.log.info("Database builder executed successfully!")
    
    def areQueriesAvailable(self):
        return len(self.queries) > 0

