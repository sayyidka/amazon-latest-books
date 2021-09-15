import logging

from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(
    filename="logs/app.log",
    level=logging.INFO,
    format="%(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("SQLManager")


class SQLManager:
    def __init__(self, db_name, db_user, db_password, db_host, db_port) -> None:
        url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        try:
            self.engine = create_engine(url)
            self.metadata = MetaData(self.engine)
            logger.info(f"Connected successfully to {db_name} database")
        except SQLAlchemyError as err:
            print(f"Error: {err}")
