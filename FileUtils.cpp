/* File utils
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "FileUtils.h"

DirIter::~DirIter() {
	if(dir != NULL) {
		closedir(dir);
		dir = NULL;
	}
}

DirIter& DirIter::operator++() {
	filename = "";
	if(dir == NULL) return *this;
	dirent* entry = readdir(dir);
	if(entry == NULL) return *this;
	filename = std::string(entry->d_name, entry->d_namlen);
	return *this;
}
