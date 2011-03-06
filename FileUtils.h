/* File utils
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef __AZ__FILEUTILS_H__
#define __AZ__FILEUTILS_H__

#include "Return.h"
#include "Endianess.h"
#include "Utils.h"

#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <string>
#include <list>

static inline Return fread_bytes(FILE* f, char* d, size_t s) {
	if(fread(d, s, 1, f) > 0) return true;
	while(s > 0) {
		while(fread(d, 1, 1, f) == 0) {
			if(feof(f))
				return "end-of-file";
			if(ferror(f))
				return "file-read-error";
		}
		++d; --s;
	}
	return true;
}

template<typename T>
static inline Return fread_bytes(FILE* f, T& d) {
	ASSERT( fread_bytes(f, &d[0], sizeof(T)/sizeof(d[0])) );
	return true;
}

template <typename T, typename _D>
static Return fread_litendian(FILE* stream, _D& d) {
	T data;
	ASSERT( fread_bytes(stream, (char*) &data, sizeof(T)) );
	EndianSwap(data);
	d = (_D)data;
	return true;
}

template <typename T, typename _D>
static Return fread_bigendian(FILE* stream, _D& d) {
	T data;
	ASSERT( fread_bytes(stream, (char*) &data, sizeof(T)) );
	BEndianSwap(data);
	d = (_D)data;
	return true;
}

struct DirIter : DontCopyTag {
	DIR* dir;
	std::string filename;
	
	DirIter(const std::string& dirname) : dir(opendir(dirname.c_str())) {}
	~DirIter();
	DirIter& operator++();
	operator bool() const { return dir != NULL && !filename.empty(); }
};


#endif
