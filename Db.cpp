/* simple DB interface
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef Z_CompressionLevel
#define Z_CompressionLevel 9
#endif

#ifndef Z_BufSize
#define Z_BufSize 1024*128
#endif

#include "Db.h"
#include "Sha1.h"
#include "StringUtils.h"
#include "FileUtils.h"
#include <cassert>
#include <zlib.h>
#include <cstdlib>

#include <iostream>
using namespace std;

void DbEntry::calcSha1() {
	sha1 = calc_sha1(data);
}

void DbEntry::compress() {
	compressed = "";
	
	z_stream stream;
	stream.zalloc = Z_NULL;
	stream.zfree = Z_NULL;
	stream.opaque = Z_NULL;
	deflateInit(&stream, Z_CompressionLevel);
	
	stream.avail_in = data.size();
	stream.next_in = (unsigned char*) &data[0];
	while(true) {
		char outputData[Z_BufSize];
		stream.avail_out = sizeof(outputData);
		stream.next_out = (unsigned char*) outputData;
		int ret = deflate(&stream, Z_FINISH);
		switch(ret) {
			case Z_OK: break;
			case Z_STREAM_END: break;
			// these cases should not happen. but check anyway
			case Z_STREAM_ERROR:
			default:
				cerr << "error to deflate " << data.size() << " bytes" << endl;
				cerr << "remaining: " << stream.avail_in << " bytes" << endl;
				cerr << "deflate ret: " << ret << endl;
				assert(false);
				return;
		}
		size_t out_size = sizeof(outputData) - stream.avail_out;
		compressed += std::string(outputData, out_size);
		if(ret == Z_STREAM_END) break;
	}
	deflateEnd(&stream);
}

Return DbEntry::uncompress() {
	data = "";
	
	z_stream stream;
	stream.zalloc = Z_NULL;
	stream.zfree = Z_NULL;
	stream.opaque = Z_NULL;
	inflateInit(&stream);
	
	bool gotStreamEnd = false;
	stream.avail_in = compressed.size();
	stream.next_in = (unsigned char*) &compressed[0];
	while(true) {
		char outputData[Z_BufSize];
		stream.avail_out = sizeof(outputData);
		stream.next_out = (unsigned char*) outputData;
		int ret = inflate(&stream, Z_NO_FLUSH);
		switch(ret) {
			case Z_STREAM_ERROR: inflateEnd(&stream); return "zlib stream error / invalid compression level";
			case Z_NEED_DICT: inflateEnd(&stream); return "zlib need dict error";
			case Z_DATA_ERROR: inflateEnd(&stream); return "zlib data error";
			case Z_MEM_ERROR: inflateEnd(&stream); return "zlib out-of-memory error";
			case Z_STREAM_END: gotStreamEnd = true;
		}
		size_t out_size = sizeof(outputData) - stream.avail_out;
		if(out_size == 0) break;
		data += std::string(outputData, out_size);
	}	
	inflateEnd(&stream);
	
	if(!gotStreamEnd)
		return "zlib stream incomplete";
	return true;
}
