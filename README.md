# [BlazingCache](http://blazingcache.org/)

BlazingCache is a Fast Cache for distributed Java applications. It is not a technology for distributed computing or for building grids but it is specialized in beeing strictly a 'cache' wich provides strong performance and consistency guarantees.

You can see BlazingCache like a group of distributed processes each of them owning a local cache, sometimes called also near-cache. BlazingCache coordinates these local caches in order to guarantee that if a member of the groups invalidates an entry or put a more recent version of a value in the cache this operation is propagated to all the other peers.

The focus is on Consistency and BlazingCache will protect you from reading stale data from cache.

BlazingCache relies on a central (replicated) coordinator service in order to achive its goals. Apache Zookeeper is used for cordination of coordinators and for service discovery.

## Getting Involved

See our [docs](https://blazingcache.readme.io)

Give us [feedback](https://dev.blazingcache.org/jira/secure/Dashboard.jspa)

Join the [mailing list](http://lists.blazingcache.org/mailman/listinfo)

## License

BlazingCache is under [Apache 2 license](http://www.apache.org/licenses/LICENSE-2.0.html).
