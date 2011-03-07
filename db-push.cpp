/* tool to push some entry to the DB
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "Png.h"
#include "DbDefBackend.h"
#include "DbPng.h"
#include "StringUtils.h"

#include <ctime>
#include <cstdlib>
#include <cstdio>
#include <iostream>
using namespace std;

int main(int argc, char** argv) {
	if(argc <= 1) {
		cerr << "please give me a filename" << endl;
		return 1;
	}
	
	std::string filename = argv[1];
	FILE* f = fopen(filename.c_str(), "rb");
	if(f == NULL) {
		cerr << "error: cannot open " << filename << endl;
		return 1;
	}
	
	srandom(time(NULL));
	DbDefBackend db;
	DbPngEntryWriter dbPngWriter(f, &db);
	while(dbPngWriter) {
		Return r = dbPngWriter.next();		
		if(!r) {
			cerr << "error: " << r.errmsg << endl;
			return 1;
		}
	}
	
	db.pushToDir("", DbDirEntry::File(baseFilename(filename), ftell(f)));
	db.setFileRef(dbPngWriter.contentId, baseFilename(filename));
	fclose(f);
	
	cout << "content id: " << hexString(dbPngWriter.contentId) << endl;
	cout << "num content entries: " << dbPngWriter.contentEntries.size() << endl;		
	cout << "db stats: push new: " << db.stats.pushNew << endl;
	cout << "db stats: push reuse: " << db.stats.pushReuse << endl;
	
	cout << "success" << endl;
	return 0;
}
