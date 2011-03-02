/* PNG parser
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "Png.h"
#include "FileUtils.h"

#include <cstring>
#include <stdint.h>

Return png_read_sig(FILE* f) {
	static const char PNGSIG[8] = {137,80,78,71,13,10,26,10};
	char sig[sizeof(PNGSIG)];
	ASSERT( fread_bytes(f, sig) );
	if(memcmp(PNGSIG, sig, sizeof(PNGSIG)) != 0)
		return "png-signature-wrong";
	return true;
}

Return png_read_chunk(FILE* f, PngChunk& chunk) {
	uint32_t len;
	ASSERT( fread_litendian<uint32_t>(f, len) );
	char type[4];
	
}
