from sqlalchemy import create_engine,Table,MetaData
from sqlalchemy.ext.declarative import as_declarative,declarative_base
from sqlalchemy.orm import sessionmaker
from api.log import logger
import settings

# Base = declarative_base()

# @as_declarative
# class Base:
#     pass

# @as_declarative
# class Base_config:
#     pass

Base = declarative_base()
Base_config = declarative_base()
Cloud = declarative_base()

total_db_config = settings.DATABASES['total']
config_db_config = settings.DATABASES['config']

SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://{total_db_config['username']}:{total_db_config['password']}@{total_db_config['host']}:{total_db_config['port']}/{total_db_config['database_name']}"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, pool_pre_ping=True
)

# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
engine_config = create_engine(f"mysql+pymysql://{config_db_config['username']}:{config_db_config['password']}@{config_db_config['host']}:{config_db_config['port']}/{config_db_config['database_name']}")

SessionLocal = sessionmaker(autocommit=False, autoflush=False)
SessionLocal.configure(binds={Base:engine,
Base_config:engine_config
})


