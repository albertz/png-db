/* simple tool to extract a single file from the DB
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

Return _main(const std::string& filename) {
	DbDefBackend db;
	db.setReadOnly(true);
	ASSERT( db.init() );
	
	DbEntryId fileEntryId;
	ASSERT( db.getFileRef(fileEntryId, filename) );
	cout << "entry id: " << hexString(fileEntryId) << endl;
	
	std::string extract_fn = baseFilename(filename);
	FILE* f = fopen(extract_fn.c_str(), "wb");
	if(f == NULL)
		return "cannot open " + extract_fn;
	
	if(!fileEntryId.empty()) {
		FileWriteCallback writer(f);
		DbPngEntryReader dbPngReader(&writer, &db, fileEntryId);
		while(dbPngReader)
			ASSERT( dbPngReader.next() );
	}
	
	cout << "wrote " << ftell(f) << " bytes" << endl;
	fclose(f);
	
	return true;
}

int main(int argc, char** argv) {
	if(argc <= 1) {
		cerr << "please give me a filename" << endl;
		return 1;
	}
	
	std::string filename = argv[1];
	srandom(time(NULL));
	Return r = _main(filename);
	if(!r) {
		cerr << "error: " << r.errmsg << endl;
		return 1;
	}	
	
	cout << "success" << endl;
	return 0;
}
