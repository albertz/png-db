/* single raw file DB backend
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "DbFileBackend.h"
#include "StringUtils.h"
#include "Utils.h"
#include "FileUtils.h"
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdint.h>

#include <iostream>
using namespace std;

DbFileBackend::~DbFileBackend() {
	if(file != NULL) {
		fclose(file);
		file = NULL;
	}
}

static const char DbFile_Signature[] = {'A','Z','P','N','G','D','B',13,10,27};

static Return __db_fwrite(DbFileBackend& db, const char* d, size_t s) {
	ASSERT( fwrite_bytes(db.file, d, s) );
	long curPos = ftell(db.file);
	if(curPos < 0)
		return "error on telling file stream possition / non-seekable stream?";
	db.fileSize = std::max((size_t)curPos, db.fileSize);
	return true;
}

static Return __db_fwrite(DbFileBackend& db, const std::string& d) {
	return __db_fwrite(db, &d[0], d.size());
}

Return DbFileBackend::init() {
	if(file != NULL) {
		fclose(file);
		file = NULL;
	}
	
	int fd = open(filename.c_str(), readonly ? O_RDONLY : (O_RDWR|O_CREAT), 0644);
	if(fd < 0)
		return "failed to open DB file " + filename;
		
	file = fdopen(fd, readonly ? "rb" : "w+b");
	if(file == NULL)
		return "failed to open DB file handle";

	fseek(file, 0, SEEK_END);
	fileSize = ftell(file);
	fseek(file, 0, SEEK_SET);
	
	if(fileSize == 0) {
		// init new file
		ASSERT( __db_fwrite(*this, DbFile_Signature, sizeof(DbFile_Signature)) );
	}
	else if(fileSize < sizeof(DbFile_Signature))
		return "DB file even too small for the signature";
	else {
		char tmp[sizeof(DbFile_Signature)];
		ASSERT( fread_bytes(file, tmp, sizeof(tmp)) );
		if(memcpy(tmp, DbFile_Signature, sizeof(tmp)) != 0)
			return "DB file signature wrong";
	}
	
	return true;
}

// creates or append to an entry
static Return __db_append(DbFileBackend& db, const std::string& key, const std::string& value) {
	// TODO ...
	return false;
}

static Return __db_set(DbFileBackend& db, const std::string& key, const std::string& value) {
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

