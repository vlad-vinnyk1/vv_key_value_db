# Key Value Database(One Node)
## Read flow:
`DatabaseNode (1)-> BloomFilter (2)-> MemoryCache (3)-> SSTable on disk.`
1. Check might Contain in Bloom Filer, if yes goes to step 2, if not returns null.
2. Check in the MemoryCache, if present return value. If not, check is SSTables(step 3).
3. I am checking in the SSTable, from the recent log files to the older ones. (Compaction is the case to limit the number of files and speed up readings).
## Updating flow:
`DatabaseNode (1)-> BloomFilter (2)-> MemoryCache(if threashold dump on SSTable)  (3)-> SSTable on disk`
#### Put/Updating:
1. Add to Bloom Filter
2. Add to MemoryCache
3. If memory cache reaches a threshold, dump to disk as logs files of the SSTable.
#### Removing:
1. Add to key value TOMB.
2. If try read with this value, return null.
3. Compaction remove the key from log.
### Compaction:
As element in log files is sorted -> using a modification of merge sort algorithms + Minimum Heap to speed up search smallest element:
Lazily iterating through elements of the log files. And merging them to one log file.

### Improvements to do
1. Memory Cache is not durable, in case of the case not being saved data will be lost, need to implement WAL in the future.
2. Make Compactor splittable.