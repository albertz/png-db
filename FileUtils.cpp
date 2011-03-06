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

void DirIter::next() {
	filename = "";
	if(dir == NULL) return;
	dirent* entry = readdir(dir);
	if(entry == NULL) return;
	filename = std::string(entry->d_name, entry->d_namlen);
}
