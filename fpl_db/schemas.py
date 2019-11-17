"""Defines SQLite database schema via SQLAlchemy ORM.

Additional documentation can be found here:
http://docs.sqlalchemy.org/en/latest/orm/extensions/declarative/basic_use.html
"""

from sqlalchemy import create_engine, Column, String, BigInteger, ForeignKey, Float, Index, Boolean, distinct
from sqlalchemy.exc import IntegrityError, StatementError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import exists

Base = declarative_base()


class FplRecords(Base):
    __tablename__ = 'fpl_records'

    sic_code = Column(String, primary_key=True)
    ad_office = Column(String)
    industry_title = Column(String)