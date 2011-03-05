/* PNG parser
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "Png.h"
#include "FileUtils.h"
#include "Crc.h"

#include <cstring>
#include <stdint.h>
#include <cassert>

#include <iostream>
using namespace std;

Return png_read_sig(FILE* f) {
	static const char PNGSIG[8] = {137,80,78,71,13,10,26,10};
	char sig[sizeof(PNGSIG)]; memset(sig, 0, sizeof(PNGSIG));
	ASSERT( fread_bytes(f, sig) );
	if(memcmp(PNGSIG, sig, sizeof(PNGSIG)) != 0)
		return "PNG signature wrong";
	return true;
}

Return png_read_chunk(FILE* f, PngChunk& chunk) {
	uint32_t len;
	ASSERT_EXT( fread_bigendian<uint32_t>(f, len), "failed to read chunk len" );
	
	char type[4];
	ASSERT_EXT( fread_bytes(f, type), "failed to read chunk type" );
	for(short i = 0; i < sizeof(type); ++i)
		if((unsigned char)type[i] < 32 || (unsigned char)type[i] >= 128)
			return "chunk type invalid";
	chunk.type = std::string(type, sizeof(type));
	
	chunk.data = std::string(len, 0);
	ASSERT_EXT( fread_bytes(f, &chunk.data[0], len), "failed to read chunk data" );
	
	uint32_t crc;
	ASSERT_EXT( fread_bigendian<uint32_t>(f, crc), "failed to read chunk crc" );
	if(crc != calc_crc(chunk.type, chunk.data))
		return "CRC does not match";
	
	return true;
}

PngReader::PngReader(FILE* f) {
	file = f;
	stream.zalloc = Z_NULL;
	stream.zfree = Z_NULL;
	stream.opaque = Z_NULL;
	stream.avail_in = 0;
	stream.next_in = Z_NULL;
	gotStreamEnd = gotEndChunk = false;
}

Return __PngReader_init(PngReader& png) {
	if(inflateInit(&png.stream) != Z_OK)
		return "failed to init inflate stream";
	return png_read_sig(png.file);
}

Return __PngReader_read(PngReader& png) {
	while(true) {
		if(png.gotEndChunk) {
			if(png.gotStreamEnd) return true;
			return "zlib data stream incomplete";
		}
		if(feof(png.file)) return "end-of-file";
		if(ferror(png.file)) return "file-read-error";
		
		PngChunk chunk;
		ASSERT( png_read_chunk(png.file, chunk) );
		bool hadEarlierDataChunk = png.dataStream.size() > 0;
		
		if(chunk.type == "IEND")
			png.gotEndChunk = true;
		else if(chunk.type == "IDAT") {
			if(png.gotStreamEnd) return "got another IDAT chunk but zlib stream was already finished";
			png.stream.avail_in = chunk.data.size();
			png.stream.next_in = (unsigned char*) &chunk.data[0];
			chunk.data = ""; // reset because we don't want to keep this in the stored chunk list
			while(true) {
				char outputData[8192];
				png.stream.avail_out = sizeof(outputData);
				png.stream.next_out = (unsigned char*) outputData;
				int ret = inflate(&png.stream, Z_NO_FLUSH);
				switch(ret) {
					case Z_STREAM_ERROR: return "zlib stream error / invalid compression level";
					case Z_NEED_DICT: return "zlib need dict error";
					case Z_DATA_ERROR: return "zlib data error";
					case Z_MEM_ERROR: return "zlib out-of-memory error";
					case Z_STREAM_END: png.gotStreamEnd = true;
				}
				size_t out_size = sizeof(outputData) - png.stream.avail_out;
				if(out_size == 0) break;
				cout << "at " << ftell(png.file) << ": got " << out_size << " uncompressed" << endl;
				png.dataStream.push_back( std::string(outputData, out_size) );
			}
		}
		
		// store the chunk. but only one single (empty) reference to IDAT.
		if(!hadEarlierDataChunk || chunk.type != "IDAT")
			png.chunks.push_back(chunk);
	}
	
	return "WTF";
}

Return PngReader::read() {
	ASSERT( __PngReader_init(*this) );
	ASSERT( __PngReader_read(*this) );
	
	(void)inflateEnd(&stream);
	return true;
}
