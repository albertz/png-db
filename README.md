DB optimized for a bunch of PNG images.

Idea
====

The idea is to split PNG images into many blocks and have each block stored in a DB.
If there are several equal blocks, it is only stored once.
Via a hash table, the lookup for such blocks is made fast.

Technical details
=================

To make things easier on the PNG side, it just parses down until it gets a scanline serialization.
Multiple directly following scanline (of same width) serializations parts build up a block
(so it actually really matches a block in the real picture). But I don't do any of the PNG filtering.
PNG spec: <http://www.w3.org/TR/PNG/>

The general DB layout is as follows:

- ("data." unique id -> zlib compressed data) data pairs
- ("sha1refs." SHA1 -> set of ids) data pairs
- ("fs." filename -> id) data pairs

Such data value (uncompressed) starts with a data-type-byte. Only 3 types are there currently:

- PNG file summary
- PNG chunk (all non-data PNG chunks)
- PNG block

There are multiple DB backend implementations:

- The filesystem itself. But creates a lot of files!
- [Redis](http://redis.io/). Via [hiredis](https://github.com/antirez/hiredis). As everything is in memory, you are a bit limited.
- [KyotoCabinet](http://fallabs.com/kyotocabinet/). (Currently the default.)

Tools
=====

It comes with several tools. Some of them:

- db-push: Pushes a single PNG into the DB.
- db-push-dir: Pushes all PNGs in a given directory into the DB.
- db-extract-file: Extracts a single PNG from the DB.
- db-fuse: Simple FUSE interface to the DB. (Slow though because it is not very optimized!)

Compilation
===========

Just run `./compile.sh`.

For Mac: If you haven't MacFUSE installed, install it from here: <http://code.google.com/p/macfuse/>

TODOs
=====

- Many parts could be optimized a lot.
- Try with other DB backend implementations.
Maybe [mongoDB](http://www.mongodb.org/) or [Basho Riak](http://www.basho.com/products_riak_overview.php).
Or improve the filesystem implementation (which is incomplete anyway currently).
- To make the FUSE interface faster, the caching must be improved.
Also, there should be a way to get the filesize in advance and maybe also to seek in constant time.
Probably, to make this possible, we need to have a fixed compression algorithm and
the file summary must contain some offset information.
- We could also store other image formats in a similar way. And also general files.
There should be also a standard fallback.
- The FUSE interface could also support writing.


-Albert Zeyer, <http://www.az2000.de>
