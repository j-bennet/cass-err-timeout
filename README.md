# Summary

This script was created to demonstrate cassandra `execute_concurrent` breaking
when inserting into more than one table using a generator.

# Exceptions

I did a few runs that produced slightly different exceptions.

Exception #1:

```
(cass-err)irina@ue1a-hack:~/scripts/cass-err$ python runner.py --mode=insert --hosts=host1.com,host2.com
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
(cass-err)irina@ue1a-hack:~/scripts/cass-err$ python runner.py --mode=insert --hosts=host1.com,host2.com
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
(cass-err)irina@ue1a-hack:~/scripts/cass-err$ python runner.py --mode=insert --hosts=host1.com,host2.com
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