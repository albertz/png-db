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
	memset(&header, sizeof(PngHeader), 0);
	incompleteScanlineOffset = 0;
	hasInitialized = gotHeader = gotStreamEnd = gotEndChunk = hasFinishedReading = false;
}

static Return __PngReader_init(PngReader& png) {
	if(inflateInit(&png.stream) != Z_OK)
		return "failed to init inflate stream";
	ASSERT( png_read_sig(png.file) );
	png.hasInitialized = true;
	return true;
}

static Return __PngReader_read_header(PngHeader& header, PngChunk& chunk) {
	if(chunk.data.size() != PngHeader::SIZE)
		return "IHDR size is invalid";
	memcpy(&header, &chunk.data[0], PngHeader::SIZE);
	BEndianSwap(header.width);
	BEndianSwap(header.height);
	return true;
}

void PngInterlacedPos::inc(uint32_t height) {
	// TODO: more efficient ...
	// see from http://www.w3.org/TR/PNG/#8Interlace
	static const int starting_row[7]  = { 0, 0, 4, 0, 2, 0, 1 };
	static const int row_increment[7] = { 8, 8, 8, 4, 4, 2, 2 };
	//static const int block_height[7]  = { 8, 8, 4, 4, 2, 2, 1 };
	//static const int block_width[7]   = { 8, 4, 4, 2, 2, 1, 1 };
	
	if(pass >= 7) return;
	
	row += row_increment[pass];
	if(row >= height) {
		++pass;
		if(pass < 7)
			row = starting_row[pass];
	}
}

size_t PngInterlacedPos::scanlineWidth(uint32_t width) {
	if(pass >= 7) return width;

	static const int starting_col[7]  = { 0, 4, 0, 2, 0, 1, 0 };
	static const int col_increment[7] = { 8, 8, 4, 4, 2, 2, 1 };
	
	size_t s = 0;
	for(long col = starting_col[pass]; col < width; col += col_increment[pass], ++s) {
		/*visit(row, col,
		 min(block_height[pass], png.header.height - row),
		 min(block_width[pass], png.header.width - col));*/
	}
	return s;
}

static Return __PngReader_fill_scanlines(PngReader& png, char* data, size_t s) {
	if(png.header.interlaceMethod > 1)
		return "invalid/unknown interlace method";

	size_t scanlineSize = png.header.scanlineSize(png.header.width);
	if(png.header.interlaceMethod == 1)
		scanlineSize = png.header.scanlineSize(png.interlacedPos.scanlineWidth(png.header.width));
	
	while(s > 0) {
		std::string& buf = png.incompleteScanline;
		if(buf.size() != scanlineSize) {
			if(png.incompleteScanlineOffset == 0)
				buf = std::string(scanlineSize, 0);
			else
				return "fill scanlines: bad state";
		}
		
		size_t nbytes_to_copy = std::min(s, scanlineSize - png.incompleteScanlineOffset);
		memcpy(&buf[png.incompleteScanlineOffset], data, nbytes_to_copy);
		png.incompleteScanlineOffset += nbytes_to_copy;
		data += nbytes_to_copy;
		s -= nbytes_to_copy;
		
		if(png.incompleteScanlineOffset == scanlineSize) {
			png.scanlines.push_back(buf);
			if(png.header.interlaceMethod == 1) {
				png.interlacedPos.inc(png.header.height);
				scanlineSize = png.header.scanlineSize(png.interlacedPos.scanlineWidth(png.header.width));
			}
			png.incompleteScanlineOffset = 0;
		}
	}
	
	return true;
}

static Return __PngReader_read_data(PngReader& png, PngChunk& chunk) {
	png.stream.avail_in = chunk.data.size();
	png.stream.next_in = (unsigned char*) &chunk.data[0];
	while(true) {
		char outputData[1024*128];
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
		ASSERT( __PngReader_fill_scanlines(png, outputData, out_size) );
	}
	return true;
}

static Return __PngReader_read(PngReader& png) {
	if(png.gotEndChunk) {
		if(png.gotStreamEnd) return "cannot read more: already got end chunk";
		return "zlib data stream incomplete";
	}
	if(feof(png.file)) return "end-of-file";
	if(ferror(png.file)) return "file-read-error";
	
	PngChunk chunk;
	ASSERT( png_read_chunk(png.file, chunk) );
	
	if(chunk.type == "IDAT") {
		if(!png.gotHeader) return "got data chunk but didn't got header";
		if(png.gotStreamEnd) return "got another IDAT chunk but zlib stream was already finished";
		ASSERT( __PngReader_read_data(png, chunk) );
		chunk.data = ""; // reset because we don't want to keep this in the stored chunk list
	}
	else if(chunk.type == "IHDR") {
		ASSERT( __PngReader_read_header(png.header, chunk) );
		png.gotHeader = true;
	}
	else if(chunk.type == "IEND")
		png.gotEndChunk = true;

	// only keep chunks with information that we don't keep otherwise somewhere else
	if(chunk.type != "IDAT" && chunk.type != "IEND")
		png.chunks.push_back(chunk);
	
	return true;
}

Return PngReader::read() {
	if(hasFinishedReading) return "cannot read more: finished already";
	if(!hasInitialized) return __PngReader_init(*this);

	ASSERT( __PngReader_read(*this) );
	if(gotStreamEnd && gotEndChunk) {
		(void)inflateEnd(&stream);
		hasFinishedReading = true;
	}
	
	return true;
}
