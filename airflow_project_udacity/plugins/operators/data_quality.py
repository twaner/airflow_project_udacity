from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """An operator to load the Fact table.

    Keyword arguments:
    redshift_conn_id -- The redshift connection string in Airflow.
    table -- The table name that will be used.
    sql -- A list of sql statements to run.
    verbose_logging -- Boolean for if logging is on.
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 verbose_logging=False,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.verbose_logging=verbose_logging
        self.sql=sql

    def execute(self, context):
        """Executes the function of the operator

        Keyword arguments:
        self -- The class.
        context -- Context that contains variables.
        """
        use_verbose = self.verbose_logging

        for table in self.table:
            if use_verbose:
                self.log.info(f"DataQualityOperator starting for {table}")

            redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            try:
                records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            except Exception as e:
                self.log.error(f"Error selecting records from {table}")

            if records and (len(records) < 1 or len(records[0]) < 1):
                self.log.error(f"Data quality check failed. {table} returned no results")
            self.log.info(f"records \n {records}")
            num_records = records[0][0]

            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")

            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")

            if use_verbose:
                self.log.info(f"Completed data quality check on {table}")
