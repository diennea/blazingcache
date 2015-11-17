# [BlazingCache](http://blazingcache.org/)

BlazingCache is a Blazing Fast Cache for distributed Java applications. It is not a technology for distributed computing or for building grids, but it is specialized in beeing a 'cache' which is insanely fast and provides strong consistency guarantees.

You can see a BlazingCache system like a group of distributed processes each of them owns a local cache, sometimes called a near-cache. BlazingCache coordinates these local caches in order to guarantee that if a member of the groups invalidates an entry or caches a more recent version of a value this operation in propagated to the other peers which cached the same value.

The focus is on Consistency, BlazingCache will protect you from reading stale data from cache.

BlazingCache relies on a central (replicated) coordinator service in order to achive its goals. Apache Zookeeper is used for cordination of coordinators and for service discovery 


## License

Blazing is under [Apache 2 license](http://www.apache.org/licenses/LICENSE-2.0.html).
