# Slow queue

[![Go Reference](https://pkg.go.dev/badge/github.com/reddec/slowqueue.svg)](https://pkg.go.dev/github.com/reddec/slowqueue)

[Slow queue](https://pkg.go.dev/github.com/reddec/slowqueue/queue) is classical FIFO queue, based on approach where one file is one element.

Designed compromises and features:

- Store as minimal as possible information in memory.
- Number of elements and element sized only limited by underlying file storage.
- Designed to handle a lot of large elements by with reasonable compromise of speed in memory-constraint environments
- Consistent and fault-tolerant.
- Zero dependencies.

Does NOT support access to the same queue from different processes, however, it is **go-routine safe**.

**Summary**: it's thread-safe consistent queue, relatively slow (~3-4k push-poll ops / second), for large elements, 
with small, fixed memory footprint, designed for embedded, edge or other memory constraint devices.

Additional helpers:

- [Processor](https://pkg.go.dev/github.com/reddec/slowqueue/processor): async tasks processor with retries and TTL