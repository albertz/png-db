/* tool to list a dir from the DB
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "Png.h"
#include "DbDefBackend.h"
#include "DbPng.h"
#include "StringUtils.h"
#include "FileUtils.h"

#include <ctime>
#include <cstdlib>
#include <cstdio>
#include <iostream>
using namespace std;

DbDefBackend* db = NULL;

static Return listDir(const std::string& path) {
	std::list<DbDirEntry> dirList;
	ASSERT( db->getDir(dirList, path) );
	
	for(std::list<DbDirEntry>::iterator i = dirList.begin(); i != dirList.end(); ++i) {
		if(i->mode & S_IFREG)
			cout << "file: ";
		else if(i->mode & S_IFDIR)
			cout << "dir: ";
		else
			cout << hexString(i->mode) << ": ";
		cout << path << "/" << i->name;
		if(i->mode & S_IFREG)
			cout << ", " << i->size << " bytes";
		else if(i->mode & S_IFDIR)
			cout << "/";
		cout << endl;
		if(i->mode & S_IFDIR)
			ASSERT( listDir(path + "/" + i->name) );
	}
	
	return true;
}

int main(int argc, char** argv) {
	srandom(time(NULL));

	DbDefBackend dbInst;
	db = &dbInst;
	Return r = listDir("");
	if(!r) {
		cerr << "error: " << r.errmsg << endl;
		return 1;
	}
	
	cout << "success" << endl;
	return 0;
}
