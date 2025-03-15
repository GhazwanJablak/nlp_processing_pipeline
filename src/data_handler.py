import logging
import time
from typing import Union, List, Dict, TypedDict
from sqlmodel import Field, create_engine, SQLModel, Session
from jinja2 import Template
import pandas as pd
from decouple import config
from queries.tweets import raw_train_tweets, raw_test_tweets

class Tweets(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    keyword: str | None = None
    location: str | None = None
    text: str 
    target: int

class DataHandler:
    """Redshift class"""

    def __init__(self, training_data: bool, verbose: bool = False):
        """Initialize Redshift class.

        Keyword arguments:
        verbose -- whether it should be verbose [bool]
        """
        self.verbose = verbose
        self.logger = logging.getLogger(__name__)
        self.redshift_uri = config("REDSHIFT_URI")
        self.training_data=training_data
        start = time.time()
        self.start_connection()
        end = time.time()
        # print(f"Time taken in connecting to database: {end-start}")
        self.logger.info("Time taken in connecting to database: %s", end - start)

    def start_connection(self):
        """Start Redshift connection."""
        if "redshift" in self.redshift_uri:
            self.conn = create_engine(
                self.redshift_uri,
                connect_args={"sslmode": "prefer"},
                echo=self.verbose,
            )
        else:
            self.conn = create_engine(self.redshift_uri, echo=self.verbose)
        # print('Connection created with redshift')
        self.logger.info("Connection created with redshift")

    def close_connection(self):
        """Close Redshift connection."""
        self.conn.dispose()
        self.logger.info("Connection closed with redshift")

    @staticmethod
    def convert_arg_to_str(arg: Union[List, str, None]) -> str:
        """Convert arbitrary argument to string.
        Return string with rendered lists.

        Keyword arguments:
        arg -- an argument [List|str|None]
        """
        arg_str = arg
        if (arg_str is not None) and (isinstance(arg_str, (str)) is False):
            if isinstance(arg_str, (list)):
                if isinstance(arg_str[0], (str)):
                    arg_str = "','".join(arg_str)
                    arg_str = f"('{arg_str}')"

                if isinstance(arg_str[0], (int)):
                    arg_str = ",".join(map(str, arg_str))
                    arg_str = f"({arg_str})"            
            else:
                raise NotImplementedError(
                    "Only lists/strings are currently supported as arguments"
                )

        return arg_str

    @staticmethod
    def prepare_query_with_template(sql_template: str, **kwargs: TypedDict) -> str:
        """Render SQL template by replacing arguments.
        Return string with rendered SQL.

        Keyword arguments:
        sql_template -- jinja2 SQL template [str]
        kwargs -- arguments to replace
        """
        for arg_name, arg_val in kwargs.items():
            kwargs[arg_name] = DataHandler.convert_arg_to_str(arg_val)
        sql_template_repl = Template(sql_template).render(**kwargs)

        return sql_template_repl
    
    def get_params(self) -> Dict:
        """Get initialized and validated parameters.
        Return dictionary with all parameters
        """

        return {
        }

    def get_raw_data(self) -> pd.DataFrame:
        """Get raw tweets.
        Return dataframe with results.
        """
        if self.training_data:
            template = raw_train_tweets
        else:
            template=raw_test_tweets

        return self.redshift_db.download_data(template, self.get_params())

    def download_data(self, sql_template: str, params: Dict) -> pd.DataFrame:
        """Get data using Jinja2 SQL template and parameters.
        Return dataframe.

        Keyword arguments:
        sql_template -- jinja2 SQL template [str]
        params -- parameters to replace in template [Dict]
        """
        if self.conn is None:
            raise RuntimeError("No available connection")

        self.logger.info("Extracting machinery data")

        start = time.time()
        query = DataHandler.prepare_query_with_template(sql_template, **params)
        with Session(self.conn) as session:
            objs: List[SQLModel] = session.exec(query).all()
            records = [i._asdict() for i in objs]
            data_df = pd.DataFrame.from_records(records)

        end = time.time()
        self.logger.info(
            "Time taken in obtain machinery data of shape %s: %s",
            data_df.shape,
            end - start,
        )

        return data_df



    def upload_data(self, df):
        """Up load processed data to database"""
        
        SQLModel.metadata.create_all(self.conn)
        
        for _, row in df.itterros():
            tweet_i = Tweets(keyword=row["keyword"], location=row["location"], text=row["text"], target=row["target"])
            with Session(self.conn) as session:
                session.add(tweet_i)
                session.commit()

