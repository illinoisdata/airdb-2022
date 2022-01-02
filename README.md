# AirIndex

Learned index on external memory model for key-value data

## Progress Tracker

- [x] Implement io module to file system
- [x] Setup unit test (github)
- [x] Setup lint (github)
- [ ] Implement static profiler
- [ ] Block-based variable-length KV store (micro pages)
  - [ ] Writer: key-bytes
  - [ ] Index: key-offset-range mapping
  - [ ] Reader: read partial in range
- [ ] Add dataset: fixed-sized elements (e.g. SOSD)
  - [ ] Index
  - [ ] Reader
- [ ] Step function
  - [ ] Model builder
  - [ ] Layer builder
- [ ] Linear function
  - [ ] Model builder
  - [ ] Layer builder
- [ ] Parallel builder connector
- [ ] Partition builder connector
- [ ] Implement delta-bounded index db