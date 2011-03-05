/* public api for steve reid's public domain SHA-1 implementation */
/* this file is in the public domain */

#ifndef __SHA1_H
#define __SHA1_H

#include <string>
#include <stdint.h>

#define SHA1_DIGEST_SIZE 20

struct SHA1_CTX {
    uint32_t state[5];
    uint32_t count[2];
    uint8_t  buffer[64];

	SHA1_CTX();
	void update(const uint8_t* data, const size_t len);
	std::string final();
};

#endif /* __SHA1_H */
