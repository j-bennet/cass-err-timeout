# Summary

This script was created to demonstrate cassandra `execute_concurrent` breaking
when inserting into more than one table using a generator.

# How to run

Create virtual environment and install requirements:

```
mkvirtualenv cass-err-timeout
workon cass-err-timeout
pip install -r requirements.txt
```

Create keyspace `testinserts` and 2 tables in it, `events_1` and `events_2`:

```
python runner.py --hosts=host1,host2 --mode=create
```

Insert 1000 records, half of them into `events_1` and the other half into `events_2`,
in one batch:

```
python runner.py --hosts=host1,host2 --mode=insert --size=1000
```

Cleanup by dropping keyspace `testinserts`:

```
python runner.py --hosts=host1,host2 --mode=cleanup
```

# Findings

* Exception only happened when using generator. I could not reproduce it when
forcing the script to use a list:

```
python runner.py --hosts=host1,host2 --mode=insert --size=1000 --use-list
```

* Exceptions are more likely to happen on a cluster with more than one contact points.
 However, I could reproduce it on a local cassandra instance by running the script
 multiple times.
 
* Exceptions are more likely with a larger batch. However, I could reproduce it with
a batch as small as 2 records.


# Exceptions

A few runs of the script produced slightly different exceptions.

Exception #1:

```
(cass-err)irina@ue1a-hack:~/scripts/cass-err$ python runner.py --mode=insert --host1,host2
Inserting 1000 events.
Traceback (most recent call last):
  File "runner.py", line 164, in <module>
    main(args)
  File "runner.py", line 157, in main
    insert_events(host_names)
  File "runner.py", line 145, in insert_events
    execute_concurrent(session, statements_and_params)
  File "cassandra/concurrent.py", line 92, in cassandra.concurrent.execute_concurrent (cassandra/concurrent.c:1601)
  File "cassandra/concurrent.py", line 199, in cassandra.concurrent.ConcurrentExecutorListResults.execute (cassandra/concurrent.c:5067)
  File "cassandra/concurrent.py", line 114, in cassandra.concurrent._ConcurrentExecutor.execute (cassandra/concurrent.c:2204)
  File "cassandra/concurrent.py", line 116, in cassandra.concurrent._ConcurrentExecutor.execute (cassandra/concurrent.c:2122)
  File "cassandra/concurrent.py", line 123, in cassandra.concurrent._ConcurrentExecutor._execute_next (cassandra/concurrent.c:2369)
  File "runner.py", line 135, in generate_insert_statements
    yield (prep_statement(session, INSERT_EVENT, event_type), event)
  File "runner.py", line 76, in prep_statement
    statement = session.prepare(query)
  File "cassandra/cluster.py", line 2218, in cassandra.cluster.Session.prepare (cassandra/cluster.c:38461)
  File "cassandra/cluster.py", line 2215, in cassandra.cluster.Session.prepare (cassandra/cluster.c:38317)
  File "cassandra/cluster.py", line 3781, in cassandra.cluster.ResponseFuture.result (cassandra/cluster.c:73073)
cassandra.OperationTimedOut: errors={<Host: 10.22.1.197 us-east>: NoConnectionsAvailable()}, last_host=10.22.1.197
```

Exception #2:

```
(cass-err)irina@ue1a-hack:~/scripts/cass-err$ python runner.py --mode=insert --hosts=host1,host2
Inserting 1000 events.
Traceback (most recent call last):
  File "runner.py", line 173, in <module>
    main(args)
  File "runner.py", line 166, in main
    insert_events(host_names)
  File "runner.py", line 154, in insert_events
    execute_concurrent(session, statements_and_params)
  File "cassandra/concurrent.py", line 92, in cassandra.concurrent.execute_concurrent (cassandra/concurrent.c:1601)
  File "cassandra/concurrent.py", line 199, in cassandra.concurrent.ConcurrentExecutorListResults.execute (cassandra/concurrent.c:5067)
  File "cassandra/concurrent.py", line 114, in cassandra.concurrent._ConcurrentExecutor.execute (cassandra/concurrent.c:2204)
  File "cassandra/concurrent.py", line 116, in cassandra.concurrent._ConcurrentExecutor.execute (cassandra/concurrent.c:2122)
  File "cassandra/concurrent.py", line 123, in cassandra.concurrent._ConcurrentExecutor._execute_next (cassandra/concurrent.c:2369)
  File "runner.py", line 144, in generate_insert_statements
    yield (prep_statement(session, INSERT_EVENT, event_type), event)
  File "runner.py", line 76, in prep_statement
    statement = session.prepare(query)
  File "cassandra/cluster.py", line 2218, in cassandra.cluster.Session.prepare (cassandra/cluster.c:38461)
  File "cassandra/cluster.py", line 2215, in cassandra.cluster.Session.prepare (cassandra/cluster.c:38317)
  File "cassandra/cluster.py", line 3781, in cassandra.cluster.ResponseFuture.result (cassandra/cluster.c:73073)
cassandra.OperationTimedOut: errors={<Host: 10.22.1.197 us-east>: ConnectionException('Host has been marked down or removed',)}, last_host=10.22.0.105
```

Exception #3:

```
(cass-err)irina@ue1a-hack:~/scripts/cass-err$ python runner.py --mode=insert --hosts=host1,host2
Inserting 1000 events.
Traceback (most recent call last):
  File "runner.py", line 182, in <module>
    main(args)
  File "runner.py", line 175, in main
    insert_events(host_names, batch_size, use_list)
  File "runner.py", line 161, in insert_events
    execute_concurrent(session, statements_and_params)
  File "cassandra/concurrent.py", line 92, in cassandra.concurrent.execute_concurrent (cassandra/concurrent.c:1601)
  File "cassandra/concurrent.py", line 199, in cassandra.concurrent.ConcurrentExecutorListResults.execute (cassandra/concurrent.c:5067)
  File "cassandra/concurrent.py", line 114, in cassandra.concurrent._ConcurrentExecutor.execute (cassandra/concurrent.c:2204)
  File "cassandra/concurrent.py", line 116, in cassandra.concurrent._ConcurrentExecutor.execute (cassandra/concurrent.c:2122)
  File "cassandra/concurrent.py", line 123, in cassandra.concurrent._ConcurrentExecutor._execute_next (cassandra/concurrent.c:2369)
  File "runner.py", line 145, in generate_insert_statements
    yield (prep_statement(session, INSERT_EVENT, event_type), event)
  File "runner.py", line 77, in prep_statement
    statement = session.prepare(query)
  File "cassandra/cluster.py", line 2218, in cassandra.cluster.Session.prepare (cassandra/cluster.c:38461)
  File "cassandra/cluster.py", line 2215, in cassandra.cluster.Session.prepare (cassandra/cluster.c:38317)
  File "cassandra/cluster.py", line 3781, in cassandra.cluster.ResponseFuture.result (cassandra/cluster.c:73073)
cassandra.cluster.NoHostAvailable: ('Unable to complete the operation against any hosts', {})
```

Exception #4:

```
(cass-err)irina@ue1a-hack:~/scripts/cass-err$ python runner.py --mode=insert --hosts=host1,host2
Inserting 1000 events.
Traceback (most recent call last):
  File "runner.py", line 182, in <module>
    main(args)
  File "runner.py", line 175, in main
    insert_events(host_names, batch_size, use_list)
  File "runner.py", line 161, in insert_events
    execute_concurrent(session, statements_and_params)
  File "cassandra/concurrent.py", line 92, in cassandra.concurrent.execute_concurrent (cassandra/concurrent.c:1601)
  File "cassandra/concurrent.py", line 199, in cassandra.concurrent.ConcurrentExecutorListResults.execute (cassandra/concurrent.c:5067)
  File "cassandra/concurrent.py", line 114, in cassandra.concurrent._ConcurrentExecutor.execute (cassandra/concurrent.c:2204)
  File "cassandra/concurrent.py", line 116, in cassandra.concurrent._ConcurrentExecutor.execute (cassandra/concurrent.c:2122)
  File "cassandra/concurrent.py", line 123, in cassandra.concurrent._ConcurrentExecutor._execute_next (cassandra/concurrent.c:2369)
  File "runner.py", line 145, in generate_insert_statements
    yield (prep_statement(session, INSERT_EVENT, event_type), event)
  File "runner.py", line 77, in prep_statement
    statement = session.prepare(query)
  File "cassandra/cluster.py", line 2218, in cassandra.cluster.Session.prepare (cassandra/cluster.c:38461)
  File "cassandra/cluster.py", line 2215, in cassandra.cluster.Session.prepare (cassandra/cluster.c:38317)
  File "cassandra/cluster.py", line 3781, in cassandra.cluster.ResponseFuture.result (cassandra/cluster.c:73073)
cassandra.OperationTimedOut: errors={<Host: 10.22.1.197 us-east>: ConnectionShutdown('Connection to 10.22.1.197 is defunct',)}, last_host=10.22.1.197
```