<p class="intro">
BlazingCache is a fast Cache for distributed Java applications.
</p>

<h2>Resiliency</h2>
BlazingCache leverages <a href="http://zookeeper.apache.org/" >Apache Zookeeper </a>for services discovery and coordination. A replicated coordinator service is used as central point of communication to ensure the consistency of cache entries between the members.

<h2>Consistency</h2>
BlazingCache will protect you from reading stale data from cache and guarantee that if a member of the group invalidates an entry or a more recent version of a value is put into the cache the operation will be propagated to all the other peers in the system.

<h2>Simplicity</h2>
BlazingCache is very small and designed to be a very fast cache from the get go. It implements a small set of features and doesn't require multicast or complex manual configurations. Since version 1.4.0 BlazingCache implements <a href="https://jcp.org/en/jsr/detail?id=107" >JSR107 JCache </a>API.
 