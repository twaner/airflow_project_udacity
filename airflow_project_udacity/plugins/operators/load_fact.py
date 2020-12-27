from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """An operator to load the Fact table.

    Keyword arguments:
    redshift_conn_id -- The redshift connection string in Airflow.
    table -- The table name that will be used.
    verbose_logging -- Boolean for if logging is on.
    create -- Boolean for if the create table statement run.
    delete -- Boolean for if the delete table statement run.
    append -- Boolean for if the insert table statement run.
    sql -- A list of sql statements to run.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 verbose_logging=False,
                 create=False,
                 delete=False,
                 append=False,
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.verbose_logging=verbose_logging
        self.create=create
        self.delete=delete
        self.append=append
        self.sql=sql

    def execute(self, context):
        """Executes the function of the operator

        Keyword arguments:
        self -- The class.
        context -- Context that contains variables.
        """
        # A list to hold the types of actions performed, e.g. Create, Delete, etc.
        action = []
        if self.verbose_logging:
            self.log.info(f"LoadFactOperator starting for {self.table}")

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # run the CREATE TABLE statment
        if self.create:
            try:
                redshift_hook.run(self.sql[0])
            except Exception as e:
                self.log.error(f"Error creating fact table: {self.table}")
                raise ValueError(f"Error creating fact table: {self.table}")
            action.append(f"Created {self.table},")
        # run the DELETE statements
        if self.delete:
            try:
                redshift_hook.run(f"DELETE FROM {self.table}")
            except Exception as e:
                self.log.error(f"Error deleting from fact table: {self.table}")
                raise ValueError(f"Error deleting from fact table: {self.table}")
            action.append(f" Deleted from {self.table},")
        # run the INSERT statement
        if self.append:
            try:
                redshift_hook.run(self.sql[1])
            except Exception as e:
                self.log.error(f"Error loading fact table: {self.table}")
                raise ValueError(f"Error load fact table: {self.table}")
            action.append(f" Loaded into {self.table}")
        
        if self.verbose_logging:
            self.log.info(f"LoadFactOperator ending for {self.table}. \
            With the following done {action}. \
            Run a data quality check to confirm data is in the table.")
