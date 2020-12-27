from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)
    s3_copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        REGION '{}'
        COMPUPDATE OFF
        STATUPDATE OFF
    """

    delete_sql = """
        DELETE FROM {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 verbose_logging=False,
                 create=False,
                 delete=False,
                 append=False,
                 json_path='',
                 sql="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.region=region
        self.verbose_logging=verbose_logging
        self.create=create
        self.append=append
        self.json_path=json_path
        self.delete=delete
        self.sql=sql

    def execute(self, context):
        use_verbose = self.verbose_logging
        if use_verbose:
            self.log.info(f"Starting StageToRedshiftOperator for {self.table}")

        # setup hooks
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        rendered_key = self.s3_key.format(**context)

        # Create table if it does not exist
        sub_str = "songs"
        self.log.info(f"TABLE  {sub_str} {self.table} {sub_str in self.table}")

        if self.create:
            if use_verbose:
                self.log.info(f"Creating {self.table}")
            try:
                redshift_hook.run(self.sql)
            except Exception as e:
                raise ValueError(f"Failure while attempting to create {self.table}")

        # Clear out data from table
        if self.delete:
            if use_verbose:
                self.log.info(f"Deleting from {self.table}")
            try:
                redshift_hook.run(self.delete_sql.format(self.table))
            except Exception as e:
                raise ValueError(f"Failure while attempting to delete {self.table}")
            
        # Copy from s3 to Redshift
        if self.append and False:
            s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
            copy_sql = self.s3_copy_sql.format(self.table, s3_path,
                    aws_credentials.access_key,aws_credentials.secret_key,
                    self.json_path, self.region)
                    
            if use_verbose:
                self.log.info(f"Copying data from {s3_path} {self.table} Starting")
            try:
                redshift_hook.run(copy_sql)
            except Exception as e:
                raise ValueError(f"Error while running COPY on {self.table}")
            if use_verbose:
                self.log.info(f"Copying data from {self.s3_bucket} {self.table} Completed")


