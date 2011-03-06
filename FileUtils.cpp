/* File utils
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "FileUtils.h"

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
