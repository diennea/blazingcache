# [BlazingCache](http://blazingcache.org/)

BlazingCache is a Fast Cache for distributed Java applications.

You can see BlazingCache like a group of distributed processes each of them owning a local cache, sometimes called also near-cache. BlazingCache coordinates these local caches in order to guarantee that if a member of the groups invalidates an entry or put a more recent version of a value in the cache this operation is propagated to all the other.

BlazingCache relies on a central (replicated) coordinator service in order to achieve its goals. [Apache Zookeeper](https://zookeeper.apache.org) is used for coordination of coordinators and for service discovery.

Since version 1.4.0 BlazingCache implements JSR107 JCache API.

## Getting Involved

See our [docs](https://blazingcache.readme.io)

Join the [mailing list](http://lists.blazingcache.org/mailman/listinfo)

## License

BlazingCache is under [Apache 2 license](http://www.apache.org/licenses/LICENSE-2.0.html).
