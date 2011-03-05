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

struct PngChunk {
	std::string type;
	std::string data;
};

Return png_read_sig(FILE* f);
Return png_read_chunk(FILE* f, PngChunk& chunk);

struct PngReader {
	FILE* file;
	z_stream stream;
	bool gotStreamEnd, gotEndChunk;
	std::list<PngChunk> chunks;
	std::list<std::string> dataStream;
	
	PngReader(FILE* f = NULL);
	Return read();
};

#endif
