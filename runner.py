# -*- coding: utf-8
"""
Cassandra insert test. Create tables and run inserts on them.

Usage:
    runner.py [--mode=<m>] [--hosts=<h>] [--size=<n>] [--use-list]

Options:
    -h --help    Show this screen.
    --mode=<m>   What to do (create|insert|cleanup) [default: insert].
    --hosts=<h>  Host names [default: localhost].
    --size=<n>   Batch size [default: 1000].
    --use-list   Use list instead of generator.
"""
import pylru
import datetime as dt

from docopt import docopt
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.concurrent import execute_concurrent


KEYSPACE_NAME = 'testinserts'


CREATE_KEYSPACE = """
CREATE KEYSPACE IF NOT EXISTS {keyspace}
    WITH replication = {{ 'class': 'SimpleStrategy',
                          'replication_factor': 2 }};
"""


DROP_KEYSPACE = """
DROP KEYSPACE IF EXISTS {keyspace};
"""


CREATE_EVENTS = """
CREATE TABLE IF NOT EXISTS {keyspace}.events_{suffix} (
    event_type text,
    period timestamp,
    ts timestamp,
    event_id text,
    d text,
    PRIMARY KEY ((event_type, period), ts, event_id)
) WITH CLUSTERING ORDER BY (ts ASC, event_id ASC)
   AND compaction = {{'class': 'LeveledCompactionStrategy'}}
   AND COMPACT STORAGE;
"""

INSERT_EVENT = """
INSERT INTO {keyspace}.events_{suffix}
            (event_type, period, ts, event_id, d)
            VALUES
            (?,          ?,      ?,  ?,        ?);
"""


def create_session(host_names):
    """Create cassandra session."""
    s = Cluster(contact_points=host_names).connect()
    s.default_timeout = 30.0
    s.row_factory = dict_factory
    return s


prepared_cache = pylru.lrucache(100)


def prep_statement(session, query_template, suffix, consistency_level=ConsistencyLevel.ONE):
    """Returns a prepared CQL statement for given template and suffix."""
    query = query_template.format(keyspace=KEYSPACE_NAME, suffix=suffix)
    lru_key = (session, query, consistency_level)
    if lru_key not in prepared_cache:
        statement = session.prepare(query)
        statement.consistency_level = consistency_level
        prepared_cache[lru_key] = statement

    return prepared_cache[lru_key]


def create_table(session, suffix):
    """
    Create new cassandra table.
    :param session: Cassandra session instance
    :type session: cassandra.cluster.Session
    :param suffix: string
    """
    print('Creating table events_{}'.format(suffix))
    query = CREATE_EVENTS.format(keyspace=KEYSPACE_NAME, suffix=suffix)
    session.execute(query)
    print('Created table events_{}'.format(suffix))


def create_keyspace(session):
    query = CREATE_KEYSPACE.format(keyspace=KEYSPACE_NAME)
    print('Creating keyspace {}.'.format(KEYSPACE_NAME))
    session.execute(query)
    print('Keyspace {} created.'.format(KEYSPACE_NAME))


def create_all(host_names):
    session = create_session(host_names)
    create_keyspace(session)
    create_table(session, "1")
    create_table(session, "2")


def drop_all(host_names):
    session = create_session(host_names)
    query = DROP_KEYSPACE.format(keyspace=KEYSPACE_NAME)
    print('Dropping keyspace {}.'.format(KEYSPACE_NAME))
    session.execute(query)
    print('Keyspace {} dropped.'.format(KEYSPACE_NAME))


def create_event(event_type, event_id):
    ts = dt.datetime.utcnow()
    period = dt.datetime(ts.year, ts.month, ts.day, ts.hour)
    return {
        'event_type': event_type,
        'period': period,
        'ts': ts,
        'event_id': 'Event ID {}'.format(event_id),
        'd': 'Data for {}'.format(event_id)
    }


def create_insert_statements(session, batch_size):
    statements = []
    for i in range(batch_size):
        event_type = 1 if i % 2 == 0 else 2
        event = create_event('type{}'.format(event_type), i)
        statements.append((prep_statement(session, INSERT_EVENT, event_type), event))
    return statements


def generate_insert_statements(session, batch_size):
    i = 0
    while i < batch_size:
        event_type = 1 if i % 2 == 0 else 2
        event = create_event('type{}'.format(event_type), i)
        yield (prep_statement(session, INSERT_EVENT, event_type), event)
        i += 1


def insert_events(host_names, batch_size=1000, use_list=False):
    """Prepare statements and insert into tables.
    :param host_names: list of strings
    :param batch_size: int
    :param use_list: whether to use list or generator for batch
    """
    session = create_session(host_names)
    if use_list:
        statements_and_params = create_insert_statements(session, batch_size)
    else:
        statements_and_params = generate_insert_statements(session, batch_size)
    print('Inserting {} events.'.format(batch_size))
    execute_concurrent(session, statements_and_params)
    print('Inserted {} events.'.format(batch_size))


def main(args):
    mode = args['--mode']
    host_names = args['--hosts'].split(',')
    batch_size = int(args['--size'])
    use_list = args['--use-list']
    if mode == 'create':
        create_all(host_names)
    elif mode == 'cleanup':
        drop_all(host_names)
    elif mode == 'insert':
        insert_events(host_names, batch_size, use_list)
    else:
        print(__doc__)


if __name__ == '__main__':
    args = docopt(__doc__)
    main(args)
