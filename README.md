# SimpleDynamo

Replicated Key-Value Storage

This application is a very simplified version of Dynamo. There are three main pieces that have been implemented:
<ul>
<li>Partitioning</li>
<li>Replication</li>
<li>Failure handling</li>
</ul>

The main goal is to provide both availability and linearizability at the same time. In other words, the implementation will always perform read and write operations successfully even under failures. At the same time, a read operation always returns the most recent value. Partitioning and replication are done exactly the way Dynamo does.
