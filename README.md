### Map lookup benchmark for Java-centric embedded storage frameworks
#### [MicroStream](https://microstream.one/) vs [VelvetDB](https://github.com/zakgof/velvetdb)

Comparing performance of a key-value lookup in 3 million records.

| Framework  | Total operation time, ms|
| ------------- | ------------- |
| MicroStream 8.01 HashMap (eager)  | 117045  |
| MicroStream 8.01 LazyHashMap  | 16405  |
| VelvetDB 0.10.2 | 1697  |