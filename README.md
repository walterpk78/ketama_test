# ketama_test
This is part of the first test requeted
We have decided to extend the basic python-memcache client to allow hosts to
be added at runtime. Sadly, for reasons unknown, our entire cache gets lost
when we add a new host.

We know this probably has something to do with consistent hashing, and a quick
search has yielded an appropriate algorithm/library that could help: ketama

Use this in conjunction with the MemcacheClient subclass found in
new_memcached.py
to yield *a better match* rate after a host has been added.
