Stream Collections  [![Build Status](https://travis-ci.org/Nitro/streamcollections.svg)](https://travis-ci.org/Nitro/streamcollections)  [![Codacy Badge](https://www.codacy.com/project/badge/13600c7400c047a6b5e77f7cabe6d8cf)](https://www.codacy.com/public/gregsilin_2761/streamcollections)
==================

[![Join the chat at https://gitter.im/Nitro/streamcollections](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/Nitro/streamcollections?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

This is an abstraction for streams as Scala collections that was developed during our workat Nitro on traversing our production S3 bucket.

The iterator is currently written on top of Play Iteratees, but can be generalized to any stream framework that works with Scala futures. Our immediate plans are to support Akka streams.

##### Presentations

This project was originally presented at [ScalaDays SF 2015](http://www.slideshare.net/GregSilin/stream-collections-scala-days).

Usage
-----
See [PlayStreamIteratorSpec](src/test/scala/PlayStreamIteratorSpec.scala) for examples.


Feature Suggestions, Bugs, Questions?
-------------------------------------
Please open an issue and someone from our team will respond shortly.

Contributing
------------
We welcome contributions!  Please open a Pull Request.
