DB optimized for a bunch of PNG images.

Idea
====

The idea is to split PNG images into many blocks and have each block stored in a DB.
If there are several equal blocks, it is only stored once.
Via a hash table, the lookup for such blocks is made fast.

Use case
========

I am collecting screenshots (for several reasons; one is to play around with machine learning / computer vision;
one example is here: <https://github.com/albertz/screenshooting>). A lot of them. :)

Right now, I have about 88k screenshots with about 77GB. And as many of them have a lot of repetitive areas
(on some days, I were making a screenshot every 10 seconds, even when not using the computer at all,
so the only changing part was the time display), I didn't wanted to waste so much space on so much repetitive data.

With this PNG DB, I have a compression rate of about 400-500%
(for the first 1k screenshots or so; probably the rate will even be higher for all of them).

This example with the screenshots is probably an extreme case (where this applies extremely well).
But I guess in many other cases where you are collecting a huge amount of PNG images
(with computer-generated content; real-world images would not work that well), you can safe some space by it.

And if this gets optimized as far as possible, it may be even faster than normal filesystem access
(because of less disk IO).

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

Comparison with other compression methods / deduplicators
=========================================================

In the beginning, I thought about using some generic image library to be able to handle just any
image type and then operate just on the raw data. This would even give me some slight better compression rate
because now, I am operating on PNGs scanline serializations and there are 5 different ways (filters) in PNG
to represent a scanline.

However, because I am storing all the data as PNG raw data in the DB, the reconstruction of the PNG should
be much faster. In the more generic case, I would have to recompress/reencode the PNG. Now I only have
to (roughly) collect and glew the parts together and run the PNG zlib over it.

Using a general deduplicator / compressor on the raw data (uncompressed PNG, TGA or BMP):
It would be based on connected chunks of data; i.e., in the image, it would mean one or many following scanlines.
But what I am doing is based on rectangular blocks in the image. So I am able to get much bigger
chunks of data which is repetitive.

Something like what is done in video compressions methods like H264: This might actually be a very good idea.
And it should be possible to just add it now to my current method.

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
