/* tool to push a dir to the DB
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

Return _main(const std::string& dirname) {
	DbDefBackend db;
	ASSERT( db.init() );
	
	DirIter dir(dirname);
	if(dir.dir == NULL)
		return "cannot open directory " + dirname;
	
	for(; dir; dir.next()) {
		if(dir.filename.size() <= 4) continue;
		if(dir.filename.substr(dir.filename.size()-4) != ".png") continue;
		std::string filename = dirname + "/" + dir.filename;
		
		DbEntryId ref;
		if(db.getFileRef(ref, "/" + baseFilename(filename)))
			// skip files we already have in DB
			continue;
		
		FILE* f = fopen(filename.c_str(), "rb");
		if(f == NULL) {
			cerr << "error: " << dir.filename << ": cannot open file" << endl;
			continue;
		}
		
		DbPngEntryWriter dbPngWriter(f, &db);
		while(dbPngWriter) {
			Return r = dbPngWriter.next();		
			if(!r) {
				cerr << "error: " << dir.filename << ": " << r.errmsg << endl;
				break;
			}
		}
		db.pushToDir("", DbDirEntry::File(baseFilename(filename), ftell(f)));
		db.setFileRef(dbPngWriter.contentId, "/" + baseFilename(filename));
		fclose(f);
		
		cout << dir.filename << ": "
		<< (100.0f * float(db.stats.pushReuse) / (db.stats.pushNew + db.stats.pushReuse)) << "%, "
		<< db.stats.pushReuse << " / " << db.stats.pushNew
		<< endl;
	}
	
	return true;
}

int main(int argc, char** argv) {
	if(argc <= 1) {
		cerr << "please give me a dirname" << endl;
		return 1;
	}
	
	srandom(time(NULL));

	std::string dirname = argv[1];
	Return r = _main(dirname);
	if(!r) {
		cerr << "error: " << r.errmsg << endl;
		return 1;
	}
	
	cout << "success" << endl;
	return 0;
}
