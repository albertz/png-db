/* PNG parser
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef __AZ__PNG_H__
#define __AZ__PNG_H__

#include "Return.h"

#include <string>
#include <list>
#include <cstdio>
#include <zlib.h>
#include <stdint.h>

struct PngChunk {
	std::string type;
	std::string data;
};

Return png_read_sig(FILE* f);
Return png_read_chunk(FILE* f, PngChunk& chunk);

struct PngHeader {
	static const size_t SIZE = 13;
	uint32_t width, height;
	uint8_t bitDepth;
	uint8_t colourType;
	uint8_t compressionMethod;
	uint8_t filterMethod;
	uint8_t interlaceMethod;
};

struct PngReader {
	FILE* file;
	z_stream stream;
	PngHeader header;
	bool hasInitialized, gotHeader, gotStreamEnd, gotEndChunk, hasFinishedReading;
	std::list<PngChunk> chunks;
	std::list<std::string> dataStream;
	
	PngReader(FILE* f = NULL);
	Return read();
};

#endif
