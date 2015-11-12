# [BlazingCache](http://blazingcache.org/)

BlazingCache is a Blazing Fast Cache for distributed Java applications. It is not a technology for distributed comuting or for building grids, but it is specialized on beeing a 'cache' which is insanely fast and provides strong consistency guarantees.

You can see a BlazingCache system like a group of distributed processes each of them owns a local cache, sometimes called a near-cache. BlazingCache coordinates this local caches in order to guarantee that if a member of the groups invalidates an entry or caches a more recent version of a value its operation in propagated to the other peers which cached the same value.


## License

Blazing is under [Apache 2 license](http://www.apache.org/licenses/LICENSE-2.0.html).
