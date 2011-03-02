/* PNG parser
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "Png.h"
#include "FileUtils.h"
#include "Crc.h"

#include <cstring>
#include <stdint.h>

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
