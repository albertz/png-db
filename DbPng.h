/* PNG data <-> DB interface
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef __AZ__DBPNG_H__
#define __AZ__DBPNG_H__

#include "Png.h"
#include "Db.h"
#include "Utils.h"
#include <cstdio>
#include <string>
#include <list>

struct DbPngEntryWriter {
	PngReader reader;
	DbIntf* db;
	std::list<DbEntryId> contentChunkEntries;
	std::list<DbEntryId> contentDataEntries;
	DbEntryId contentId;
	
	DbPngEntryWriter(FILE* f, DbIntf* _db) : reader(f), db(_db) {}
	Return next();
	operator bool() const { return !reader.hasFinishedReading; }
};

struct DbPngEntryBlockList {
	uint8_t blockHeight;
	size_t scanlineWidth;
	std::list<std::string> blocks;
	DbPngEntryBlockList() : scanlineWidth(0), blockHeight(0) {}
};

struct DbPngEntryReader {
	PngWriter writer;
	DbIntf* db;
	DbEntryId contentId;
	std::list<DbEntryId> contentEntries;
	bool haveContentEntries;
	DbPngEntryBlockList blockList;
	
	DbPngEntryReader(WriteCallbackIntf* w, DbIntf* _db, const DbEntryId& _contentId)
	: writer(w), db(_db), contentId(_contentId), haveContentEntries(false) {}
	Return next();
	operator bool() const { return !writer.hasFinishedWriting; }
};

#endif
