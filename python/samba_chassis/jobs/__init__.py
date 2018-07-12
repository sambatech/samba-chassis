#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 12-JUL-2018
updated_at: 12-JUL-2018

CREATE TABLE jobs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    status VARCHAR(255) NOT NULL,
    data JSON,
    application VARCHAR(255) NOT NULL,
    meta VARCHAR(255),
    finished BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);
"""
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import Column
from sqlalchemy.types import TIMESTAMP, String, Integer, JSON, Time, Boolean
from sqlalchemy.sql import func
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm.exc import NoResultFound

from samba_chassis import logging, config

_logger = logging.get(__name__)
_config = None

config_layout = config.ConfigLayout({
    "name": config.ConfigItem(
        default="tasks",
        type=str,
        rules=[lambda x: True if x.lower() == x else False]
    ),
    "project": config.ConfigItem(
        default="project",
        type=str,
        rules=[lambda x: True if x.lower() == x else False]
    ),
    "user": config.ConfigItem(
        type=str
    ),
    "password": config.ConfigItem(
        type=str
    ),
    "host_ip": config.ConfigItem(
        default='127.0.0.1',
        type=str
    ),
    "port": config.ConfigItem(
        default=3306,
        type=int
    ),
    "database": config.ConfigItem(
        default="jobs",
        type=str
    )
})

_Base = declarative_base()
_Session = None


def config(config_object=None):
    """
    Configure module.

    :param config_object: A configuration object.
    """
    global _config
    if config_object is None:
        _config = config_layout.get(base="tasks")
    else:
        _config = config_layout.get(config_object=config_object)

    global _Session
    _Session = _get_session_class()


def _get_session_class():
    engine = create_engine(
        'mysql+pymysql://{user}:{password}@{host_ip}:{port}/{database}'.format(
            user=_config.user.strip('\n').strip(' '),
            password=_config.password.strip('\n').strip(' '),
            host_ip=_config.host_ip,
            port=_config.port,
            database=_config.database
        )
    )
    return sessionmaker(bind=engine)


class Job(_Base):
    __tablename__ = 'jobs'

    id = Column(Integer, primary_key=True)
    status = Column(String(255), server_default="HELP", nullable=False)
    data = Column(JSON)
    application = Column(String(255), nullable=False)
    meta = Column(String(255), nullable=True)
    finished = Column(Boolean, default=False)
    created_at = Column(TIMESTAMP, server_default=func.utc_timestamp())
    updated_at = Column(TIMESTAMP, server_default=func.utc_timestamp(), server_onupdate=func.utc_timestamp())

    def __repr__(self):
        return "<Job(id='%s', status=%s)" % (self.id, self.status)


class FinishedJobError(InvalidRequestError):
    """A finished job update was required."""


def db_session(f):
    """Decorator for functions that use the database."""
    def wrapper(*arg, **kwargs):
        session = _Session()
        try:
            res = f(session, *arg, **kwargs)
            session.commit()
            return res
        except:
            session.rollback()
        finally:
            session.close()
    return wrapper


def to_dict(row):
    """Translates an SQLAlchemy row into a dictionary"""
    data = row.__dict__.copy()
    data.pop('_sa_instance_state')
    return data


@db_session
def create(session, status, data={}, metadata=''):
    """Create job."""
    result = Job(status=status, data=data, meta=metadata, application="{}_{}".format(_config.project, _config.name))
    session.add(result)
    session.flush()
    return to_dict(result)


@db_session
def get(session, job_id):
    """Get job data."""
    try:
        result = session.query(Job).filter(Job.id == job_id).one()
        return to_dict(result)
    except InvalidRequestError:
        return None


@db_session
def update(session, job_id, new_status=None, new_data=None, append_data=False, end=False):
    """Update job state"""
    job = session.query(Job).filter(Job.id == job_id).one()
    if job.finished:
        _logger.warning('JOB_ALREADY_FINISHED: {}'.format(job_id), extra={"job_id": job_id})
        return to_dict(job)

    if new_status is not None:
        job.status = new_status

    if new_data is not None:
        if append_data:
            new_data.update(job.data)
            job.data = new_data
        else:
            job.data = new_data.copy()

    if end:
        job.finished = True

    return to_dict(job)


@db_session
def end(session, job_id):
    """Finish job."""
    job = session.query(Job).filter(Job.id == job_id).one()
    if job.finished:
        _logger.warning('JOB_TRACKER_END_FINISHED_JOB_FAILED: {}'.format(job_id), extra={"job_id": job_id})
    else:
        job.finished = True

    return to_dict(job)


@db_session
def delete(session, job_id):
    """Delete job."""
    try:
        job = session.query(Job).filter(Job.id == job_id).one()
        session.delete(job)
    except NoResultFound:
        _logger.warning('DELETE_JOB_NOT_FOUND: {}'.format(job_id), extra={"job_id": job_id})


@db_session
def ready(session):
    """Check if database is ready for use. JSON status response (OK or ERROR)"""
    try:
        result = session.query(Job).limit(1).all()
        if result.__len__() <= 1:
            return {"DATABASE": "OK"}
    except:
        _logger.exception('DATABASE_READY_CHECK')

    return {"DATABASE": "ERROR"}
