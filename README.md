# AirIndex

Learned index on external memory model for key-value data

## Progress Tracker

- [x] Implement io module to file system
- [x] Setup unit test (github)
- [x] Setup lint (github)
- [ ] Implement static profiler
- [x] Block-based variable-length KV store (micro pages)
  - [x] Writer: key-bytes
  - [x] Reader: read partial in range
- [x] Add dataset: fixed-sized elements (e.g. SOSD)
- [ ] Step function
  - [ ] Model builder
  - [ ] Layer builder
- [x] Linear function
  - [x] Model builder
  - [x] Layer builder
- [ ] Stack and balance index builder
- [ ] Parallel builder connector
- [ ] Partition builder connector
- [ ] Implement delta-bounded index db

## Optimization List

- [ ] Async IO
- [ ] Zero-copy block store reads