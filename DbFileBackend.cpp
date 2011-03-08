/* single raw file DB backend
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "DbFileBackend.h"
#include "StringUtils.h"
#include "Utils.h"
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <iostream>
using namespace std;

DbFileBackend::~DbFileBackend() {
	if(file != NULL) {
		fclose(file);
		file = NULL;
	}
}

Return DbFileBackend::init() {
	if(file != NULL) fclose(file);

	int fd = open(filename.c_str(), readonly ? O_RDONLY : O_RDWR);
	if(fd < 0)
		return "failed to open DB file " + filename;
		
	file = fdopen(fd, readonly ? "rb" : "w+b");
	if(file == NULL)
		return "failed to open DB file handle";

	return true;
}

static Return __db_append(DbFileBackend& db, const std::string& key, const std::string& value) {
	// TODO ...
	return false;
}

static Return __db_get(DbFileBackend& db, const std::string& key, /*out*/ std::string& value) {
	// TODO ...
	return false;
}

// adds. if it exists, it fails
static Return __db_add(DbFileBackend& db, const std::string& key, const std::string& value) {
	// TODO ...
	return false;
}

static Return __db_set(DbFileBackend& db, const std::string& key, const std::string& value) {
	// TODO ...
	return false;
}


static Return __addEntryToList(DbFileBackend& db, const std::string& key, const std::string& entry) {
	if(entry.size() > 255)
		return "cannot add entries with size>255 to list";	
	ASSERT( __db_append(db, key, rawString<uint8_t>(entry.size()) + entry) );
	return true;
}

static Return __getEntryList(DbFileBackend& db, const std::string& key, std::list<std::string>& entries) {
	std::string value;
	ASSERT( __db_get(db, key, value) );
	
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

static Return __saveNewDbEntry(DbFileBackend& db, DbEntryId& id, const std::string& content) {
	unsigned short triesNum = (id.size() <= 4) ? (2 << id.size()) : 64;
	for(unsigned short i = 0; i < triesNum; ++i) {
		DbEntryId newId = id;
		newId += (char)random();
		std::string key = "data." + newId;
		if(__db_add(db, key, content)) {
			id = newId;
			return true;
		}
	}
	
	id += (char)random();
	return __saveNewDbEntry(db, id, content);
}

Return DbFileBackend::push(/*out*/ DbEntryId& id, const DbEntry& entry) {
	if(!entry.haveSha1())
		return "DB push: entry SHA1 not calculated";
	if(!entry.haveCompressed())
		return "DB push: entry compression not calculated";
	
	// search for existing entry
	std::string sha1refkey = "sha1ref." + entry.sha1;
	std::list<std::string> sha1refs;
	if(__getEntryList(*this, sha1refkey, sha1refs))
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
	ASSERT( __saveNewDbEntry(*this, id, entry.compressed) );
	
	// create sha1 ref
	ASSERT( __addEntryToList(*this, sha1refkey, id) );
	
	stats.pushNew++;
	return true;
}

Return DbFileBackend::get(/*out*/ DbEntry& entry, const DbEntryId& id) {
	std::string key = "data." + id;
	ASSERT( __db_get(*this, key, entry.compressed) );
	
	ASSERT( entry.uncompress() );
	entry.calcSha1();
	
	return true;
}

Return DbFileBackend::pushToDir(const std::string& path, const DbDirEntry& dirEntry) {
	std::string key = "fs." + path;
	std::string dirEntryRaw = dirEntry.serialized();
	ASSERT( __addEntryToList(*this, key, dirEntryRaw) );
	return true;
}

Return DbFileBackend::getDir(/*out*/ std::list<DbDirEntry>& dirList, const std::string& path) {
	std::string key = "fs." + path;
	std::list<std::string> entries;
	ASSERT( __getEntryList(*this, key, entries) );
	for(std::list<std::string>::iterator i = entries.begin(); i != entries.end(); ++i)
		dirList.push_back( DbDirEntry::FromSerialized(*i) );
	
	return true;
}

Return DbFileBackend::setFileRef(/*can be empty*/ const DbEntryId& id, const std::string& path) {
	std::string key = "fs." + path;
	ASSERT( __db_set(*this, key, id) );
	return true;
}

Return DbFileBackend::getFileRef(/*out (can be empty)*/ DbEntryId& id, const std::string& path) {
	std::string key = "fs." + path;
	ASSERT( __db_get(*this, key, id) );
	return true;
}

