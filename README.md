# AirIndex

Learned index on external memory model for key-value data

## Progress Tracker

- [x] Implement io module to file system
- [x] Setup unit test (github)
- [x] Setup lint (github)
- [ ] Static storage profiler
- [x] Block-based variable-length store (micro pages)
  - [x] Writer: key-bytes
  - [x] Reader: read partial in range
- [x] Fix-length array store
- [x] Add dataset: fixed-sized elements (e.g. SOSD)
- [ ] Step function
  - [ ] Model builder
  - [ ] Layer builder
- [x] Linear function
  - [x] Model builder
  - [x] Layer builder
- [x] Stack and balance index builder
- [ ] Parallel builder connector
- [ ] Partition builder connector
- [x] Meta-serializable structs
- [x] Rank DB
- [ ] Key-value DB with writing

## Optimization List

- [ ] Async IO
- [ ] Zero-copy block store reads
- [ ] IO buffer pool (or mmap)
- [ ] Fixed-size bundling of linear functions

## Refactoring List

- [ ] Metaserde cleaner pattern
