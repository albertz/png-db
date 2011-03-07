/* File utils
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "FileUtils.h"
#include <sys/stat.h> // mkdir
#include <errno.h>
#include <cstdio>

#include <iostream>
using namespace std;

Return fread_all(FILE* fp, std::string& out) {
	fseek(fp, 0, SEEK_END);
	size_t size = ftell(fp);
	fseek(fp, 0, SEEK_SET);

	out = std::string(size, '\0');
	char* outP = &out[0];
	size_t remaining = size;
	
	while(remaining > 0) {
		if(feof(fp))
			return "end-of-file";
		if(ferror(fp))
			return "file-read-error";
		size_t n = fread(outP, 1, remaining, fp);
		remaining -= n;
		outP += n;
	}
	
	return true;
}

Return fwrite_bytes(FILE* fp, const char* d, size_t size) {
	while(size > 0) {
		if(ferror(fp))
			return "file-write-error";
		size_t n = fwrite(d, 1, size, fp);
		d += n;
		size -= n;
	}
	
	return true;	
}

Return fwrite_all(FILE* fp, const std::string& in) {
	return fwrite_bytes(fp, &in[0], in.size());
}

DirIter::~DirIter() {
	if(dir != NULL) {
		closedir(dir);
		dir = NULL;
	}
}

void DirIter::next() {
	filename = "";
	if(dir == NULL) return;
	dirent* entry = readdir(dir);
	if(entry == NULL) return;
	filename = std::string(entry->d_name, entry->d_namlen);
}

static Return __createDir(const std::string& dir, mode_t mode = 0777) {
	if(mkdir(dir.c_str(), mode) != 0) {
		if(errno == EEXIST) return true; // no error
		return std::string() + "cannot create dir '" + dir + "': " + strerror(errno);
	}
	return true;
}

Return createRecDir(const std::string& abs_filename, bool last_is_dir) {
	std::string tmp;
	std::string::const_iterator f = abs_filename.begin();
	for(tmp = ""; f != abs_filename.end(); f++) {
		if(*f == '\\' || *f == '/')
			ASSERT( __createDir(tmp) );
		tmp += *f;
	}
	if(last_is_dir)
		ASSERT( __createDir(tmp) );
	return true;
}
