/* KyotoCabinet DB backend
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "DbKyotoBackend.h"
#include "StringUtils.h"
#include "Utils.h"
#include <cstdio>

#include <iostream>
using namespace std;
using namespace kyotocabinet;

DbKyotoBackend::~DbKyotoBackend() {
}

Return DbKyotoBackend::init() {
	if(!db.open(filename, PolyDB::OWRITER | PolyDB::OCREATE))
		return std::string() + "failed to open KyotoCabinet DB: " + db.error().name();
	return true;
}

typedef PolyDB KyotoDB;

static Return __addEntryToList(KyotoDB& db, const std::string& key, const std::string& entry) {
	if(entry.size() > 255)
		return "cannot add entries with size>255 to list";	
	if(!db.append(key, rawString<uint8_t>(entry.size()) + entry))
	   return std::string() + "error adding entry to list: " + db.error().name();
	return true;
}

static Return __getEntryList(KyotoDB& db, const std::string& key, std::list<std::string>& entries) {
	std::string value;
	if(!db.get(key, &value))
		return std::string() + "error getting entry list: " + db.error().name();
	
	size_t i = 0;
	while(i < value.size()) {
		uint8_t size = value[i];
		++i;
		if(i + size > value.size())
			return "entry list data is inconsistent";
		entries.push_back( value.substr(i, size) );
		i += size;
	}
	
	return true;
}

static Return __saveNewDbEntry(KyotoDB& db, DbEntryId& id, const std::string& content) {
	unsigned short triesNum = (id.size() <= 4) ? (2 << id.size()) : 64;
	for(unsigned short i = 0; i < triesNum; ++i) {
		DbEntryId newId = id;
		newId += (char)random();
		std::string key = "data." + newId;
		if(db.add(key, content)) {
			id = newId;
			return true;
		}
	}
	
	id += (char)random();
	return __saveNewDbEntry(db, id, content);
}

Return DbKyotoBackend::push(/*out*/ DbEntryId& id, const DbEntry& entry) {
	if(!entry.haveSha1())
		return "DB push: entry SHA1 not calculated";
	if(!entry.haveCompressed())
		return "DB push: entry compression not calculated";
	
	// search for existing entry
	std::string sha1refkey = "sha1ref." + entry.sha1;
	std::list<std::string> sha1refs;
	if(__getEntryList(db, sha1refkey, sha1refs))
		for(std::list<std::string>::iterator i = sha1refs.begin(); i != sha1refs.end(); ++i) {
			DbEntryId otherId = *i;
			DbEntry otherEntry;
			if(get(otherEntry, otherId)) {
				if(entry == otherEntry) {
					// found
					id = otherId;
					stats.pushReuse++;
					return true;
				}
			}
		}

	// write DB entry
	id = "";
	ASSERT( __saveNewDbEntry(db, id, entry.compressed) );
	
	// create sha1 ref
	ASSERT( __addEntryToList(db, sha1refkey, id) );
	
	stats.pushNew++;
	return true;
}

Return DbKyotoBackend::get(/*out*/ DbEntry& entry, const DbEntryId& id) {
	std::string key = "data." + id;
	if(!db.get(key, &entry.compressed))
		return std::string() + "DB get: error getting entry: " + db.error().name();
	
	ASSERT( entry.uncompress() );
	entry.calcSha1();
	
	return true;
}

Return DbKyotoBackend::pushToDir(const std::string& path, const DbDirEntry& dirEntry) {
	std::string key = "fs." + path;
	std::string dirEntryRaw = dirEntry.serialized();
	ASSERT( __addEntryToList(db, key, dirEntryRaw) );
	return true;
}

Return DbKyotoBackend::getDir(/*out*/ std::list<DbDirEntry>& dirList, const std::string& path) {
	std::string key = "fs." + path;
	std::list<std::string> entries;
	ASSERT( __getEntryList(db, key, entries) );
	for(std::list<std::string>::iterator i = entries.begin(); i != entries.end(); ++i)
		dirList.push_back( DbDirEntry::FromSerialized(*i) );
	
	return true;
}

Return DbKyotoBackend::setFileRef(/*can be empty*/ const DbEntryId& id, const std::string& path) {
	std::string key = "fs." + path;
	if(!db.set(key, id))
		return std::string() + "DB setFileRef: error setting entry: " + db.error().name();
	return true;
}

Return DbKyotoBackend::getFileRef(/*out (can be empty)*/ DbEntryId& id, const std::string& path) {
	std::string key = "fs." + path;
	if(!db.get(key, &id))
		return std::string() + "DB getFileRef: error getting entry: " + db.error().name();
	return true;
}

