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
	
	uint8_t samplesPerPixel() {
		switch(colourType) {
			case 0: return 1; // greyscale
			case 2: return 3; // truecolour
			case 3: return 1; // indexed
			case 4: return 2; // greyscale+alpha
			case 6: return 4; // truecolour+alpha
		}
		return 1; // error anyway; return 1 to avoid infinite loops
	}
	
	uint8_t bytesPerPixel() {
		return (samplesPerPixel() * bitDepth + 7) / 8;
	}
	
	uint32_t scanlineSize(uint32_t width) {
		return (width * samplesPerPixel() * bitDepth + 7) / 8 + /* filter type byte */ 1;
	}
};

struct PngInterlacedPos {
	short pass;
	uint32_t row;
	PngInterlacedPos() : pass(0), row(0) {}
	void inc(uint32_t height);
	size_t scanlineWidth(uint32_t width);
};

struct PngReader {
	FILE* file;
	z_stream stream;
	PngHeader header;
	bool hasInitialized, gotHeader, gotStreamEnd, gotEndChunk, hasFinishedReading;
	std::list<PngChunk> chunks;
	
	PngInterlacedPos interlacedPos;
	std::string incompleteScanline;
	size_t incompleteScanlineOffset;
	std::list<std::string> scanlines;
	
	PngReader(FILE* f = NULL);
	Return read();
};

#endif
