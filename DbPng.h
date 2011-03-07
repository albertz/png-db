/* PNG data <-> DB interface
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef __AZ__DBPNG_H__
#define __AZ__DBPNG_H__

#include "Png.h"
#include "Db.h"
#include <cstdio>

struct DbPngEntryWriter {
	PngReader reader;
	DbIntf* db;
	std::list<DbEntryId> contentEntries;
	DbEntryId contentId;
	
	DbPngEntryWriter(FILE* f, DbIntf* _db) : reader(f), db(_db) {}
	Return next();
	operator bool() const { return !reader.hasFinishedReading; }
};

#endif
