/* single raw file DB backend
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "DbFileBackend.h"
#include "StringUtils.h"
#include "Utils.h"
#include "FileUtils.h"
#include "Crc.h"

#include <algorithm>
#include <utility>
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdint.h>

#include <iostream>
using namespace std;

static Return __db_ftell(DbFileBackend& db, /*out*/ size_t& pos) {
	long p = ftell(db.file);
	if(p < 0)
		return "error on telling file stream possition / non-seekable stream?";
	pos = p;
	return true;
}

static Return __db_fseek(DbFileBackend& db, size_t pos) {
	if(fseek(db.file, pos, SEEK_SET) < 0)
		return "error seeking in file stream";
	return true;
}

static Return __db_fwrite(DbFileBackend& db, const char* d, size_t s) {
	ASSERT( fwrite_bytes(db.file, d, s) );
	size_t curPos = 0;
	ASSERT( __db_ftell(db, curPos) );
	db.fileSize = std::max(curPos, db.fileSize);
	return true;
}

static Return __db_fwrite(DbFileBackend& db, const std::string& d) {
	return __db_fwrite(db, &d[0], d.size());
}

#define ChunkType_Tree 1
#define ChunkType_Value 2
#define ChunkType_FreeSpace 255 // not used/implemented currently


struct DbFile_ValueChunk {
	DbFile_ValueChunk() : selfOffset(0), initialSize(0), refNextValueChunk(0), hasBeenWritten(false) {}
	size_t selfOffset;
	std::string data;
	uint32_t initialSize;
	uint64_t refNextValueChunk;
	bool hasBeenWritten;
	
	Return appendData(DbFileBackend& db, const std::string& d) {		
		if(!hasBeenWritten) {
			if(refNextValueChunk != 0) return "appenddata: has not been written but refnext!=0";
			data += d;
			ASSERT( write(db) );
			return true;
		}

		if(refNextValueChunk != 0) {
			DbFile_ValueChunk chunk;
			ASSERT( chunk.read(db, refNextValueChunk) );
			return chunk.appendData(db, d);
		}
		
		if(data.size() + d.size() <= initialSize) {
			data += d;
			ASSERT( write(db) );
			return true;
		}
		
		DbFile_ValueChunk chunk;
		chunk.selfOffset = refNextValueChunk = db.fileSize;
		ASSERT( write(db) ); // write because of new refNextValueChunk
		return chunk.appendData(db, d);
	}
	
	Return overwriteData(DbFileBackend& db, const std::string& d) {
		if(!hasBeenWritten) {
			data = d;
			ASSERT( write(db) );
			return true;
		}
		
		if(d.size() <= initialSize) {
			data = d;
			refNextValueChunk = 0; // NOTE: loosing chunk ref here!
			ASSERT( write(db) );
			return true;
		}
		
		data = d.substr(0, initialSize);
		ASSERT( write(db) );

		DbFile_ValueChunk chunk;
		if(refNextValueChunk == 0) {
			chunk.selfOffset = db.fileSize;
			ASSERT( chunk.overwriteData(db, d.substr(initialSize)) );
			refNextValueChunk = chunk.selfOffset;
			ASSERT( write(db) ); // write again because of changed refNextValueChunk
		} else {
			ASSERT( chunk.read(db, refNextValueChunk) );
			ASSERT( chunk.overwriteData(db, d.substr(initialSize)) );
		}
		
		return true;
	}
	
	Return getData(DbFileBackend& db, /*out*/std::string& d) {
		if(refNextValueChunk == 0) { d = data; return true; }
		DbFile_ValueChunk chunk;
		ASSERT( chunk.read(db, refNextValueChunk) );
		std::string nextData;
		ASSERT( chunk.getData(db, nextData) );
		d = data + nextData;
		return true;
	}
	
	Return write(DbFileBackend& db) {
		if(!hasBeenWritten) {
			initialSize = data.size();
			hasBeenWritten = true;
		}
		ASSERT( __db_fseek(db, selfOffset) );
		ASSERT( __db_fwrite(db, rawString<uint8_t>(ChunkType_Value)) );
		ASSERT( __db_fwrite(db, rawString<uint32_t>(data.size())) );
		ASSERT( __db_fwrite(db, data) );
		ASSERT( __db_fwrite(db, rawString<uint32_t>(initialSize)) );		
		ASSERT( __db_fwrite(db, rawString<uint64_t>(refNextValueChunk)) );		
		ASSERT( __db_fwrite(db, rawString<uint32_t>(calc_crc(data,
															 rawString<uint32_t>(initialSize) +
															 rawString<uint64_t>(refNextValueChunk)
															 ))) );
		return true;
	}
	
	Return read(DbFileBackend& db, size_t _selfOffset) {
		selfOffset = _selfOffset;
		hasBeenWritten = true;
		ASSERT( __db_fseek(db, selfOffset) );
		
		uint8_t chunkType = 0;
		ASSERT( fread_bigendian<uint8_t>(db.file, chunkType) );
		if(chunkType != ChunkType_Value) return "ValueChunk read: chunk type invalid";
		
		size_t len = 0;
		ASSERT( fread_bigendian<uint32_t>(db.file, len) );
		
		data = std::string(len, '\0');
		ASSERT( fread_bytes(db.file, &data[0], data.size()) );
		
		ASSERT( fread_bigendian<uint32_t>(db.file, initialSize) );
		if(initialSize < len) return "ValueChunk read: initialSize invalid";
		
		ASSERT( fread_bigendian<uint64_t>(db.file, refNextValueChunk) );

		uint32_t crc = 0;
		ASSERT( fread_bigendian<uint32_t>(db.file, crc) );
		
		if(crc != calc_crc(data, rawString<uint32_t>(initialSize) + rawString<uint64_t>(refNextValueChunk)))
			return "CRC missmatch on ValueChunk read";
		
		return true;
	}
};

struct DbFile_TreeChunk {
	size_t selfOffset;
	DbFile_TreeChunk* parent;
	uint64_t valueRef;
	uint64_t subtreeRefs[256];
	
	DbFile_TreeChunk() {
		selfOffset = 0;
		parent = NULL;
		valueRef = 0;
		memset(subtreeRefs, 0, sizeof(subtreeRefs));		
	}
	
	Return getValue(DbFileBackend& db, const std::string& key, /*out*/DbFile_ValueChunk& value,
					bool createIfNotExist = true, bool mustCreateNew = false) {
		if(key == "") {
			if(valueRef != 0) {
				if(mustCreateNew) return "entry does already exist";
				ASSERT( value.read(db, valueRef) );
				return true;
			}
			if(!createIfNotExist) return "entry not found: not set";
			valueRef = value.selfOffset = db.fileSize;
			ASSERT( write(db) );
			return true;
		}
		
		uint8_t next = (uint8_t)key[0];
		if(subtreeRefs[next] != 0) {
			DbFile_TreeChunk subtree;
			subtree.parent = this;
			ASSERT( subtree.read(db, subtreeRefs[next]) );
			return subtree.getValue(db, key.substr(1), value, createIfNotExist, mustCreateNew);
		}
		
		if(!createIfNotExist) return "entry not found: subtree does not exist";
		
		DbFile_TreeChunk subtree;
		subtree.parent = this;
		subtreeRefs[next] = subtree.selfOffset = db.fileSize;
		ASSERT( subtree.write(db) );
		ASSERT( write(db) );
		return subtree.getValue(db, key.substr(1), value, createIfNotExist, mustCreateNew);
	}
		
	Return write(DbFileBackend& db) {
		ASSERT( __db_fseek(db, selfOffset) );
		ASSERT( __db_fwrite(db, rawString<uint8_t>(ChunkType_Tree)) );

		std::string data;		
		data += rawString<uint64_t>(valueRef);
		for(short i = 0; i < 256; ++i)
			data += rawString<uint64_t>(subtreeRefs[i]);
		
		data += rawString<uint32_t>(calc_crc(data));
		data = rawString<uint32_t>(data.size() - sizeof(uint32_t)) + data;
		
		return __db_fwrite(db, data);
	}
	
	Return read(DbFileBackend& db, size_t _selfOffset) {
		selfOffset = _selfOffset;
		ASSERT( __db_fseek(db, selfOffset) );

		uint8_t chunkType = 0;
		ASSERT( fread_bigendian<uint8_t>(db.file, chunkType) );
		if(chunkType != ChunkType_Tree) return "TreeChunk read: chunk type invalid";
		
		size_t len = 0;
		ASSERT( fread_bigendian<uint32_t>(db.file, len) );
		if(len != sizeof(uint64_t) * (/*valueRef*/1 + /*subtreeRefs*/256))
			return "TreeChunk data size missmatch";
		
		std::string data(len, '\0');
		ASSERT( fread_bytes(db.file, &data[0], data.size()) );
		
		uint32_t crc = 0;
		ASSERT( fread_bigendian<uint32_t>(db.file, crc) );
		
		if(crc != calc_crc(data))
			return "CRC missmatch on TreeChunk read";
		
		uint32_t offset = 0;
		valueRef = valueFromRaw<uint64_t>(&data[offset]);
		offset += sizeof(uint64_t);

		for(short i = 0; i < 256; ++i) {
			subtreeRefs[i] = valueFromRaw<uint64_t>(&data[offset]);
			offset += sizeof(uint64_t);
		}
		
		return true;
	}
	
};

static const char DbFile_Signature[] = {137,'A','Z','P','N','G','D','B',13,10,26,10};
#define TreeRootOffset sizeof(DbFile_Signature)

void DbFileBackend::reset() {
	if(file != NULL) {
		fclose(file);
		file = NULL;
	}
	
	if(rootChunk != NULL) {
		delete rootChunk;
		rootChunk = NULL;
	}
}

Return DbFileBackend::init() {
	reset();
	
	int fd = open(filename.c_str(), readonly ? O_RDONLY : (O_RDWR|O_CREAT), 0644);
	if(fd < 0)
		return "failed to open DB file " + filename;
		
	file = fdopen(fd, readonly ? "rb" : "w+b");
	if(file == NULL) {
		close(fd);
		return "failed to open DB file handle";
	}
	
	fseek(file, 0, SEEK_END);
	fileSize = ftell(file);
	fseek(file, 0, SEEK_SET);

	rootChunk = new DbFile_TreeChunk();
	
	if(fileSize == 0) {
		// init new file
		ASSERT( __db_fwrite(*this, DbFile_Signature, sizeof(DbFile_Signature)) );
		rootChunk->selfOffset = TreeRootOffset;
		ASSERT( rootChunk->write(*this) );
	}
	else if(fileSize < sizeof(DbFile_Signature))
		return "DB file even too small for the signature";
	else {
		char tmp[sizeof(DbFile_Signature)];
		ASSERT( fread_bytes(file, tmp, sizeof(tmp)) );
		if(memcmp(tmp, DbFile_Signature, sizeof(tmp)) != 0)
			return "DB file signature wrong";
		
		ASSERT_EXT( rootChunk->read(*this, TreeRootOffset), "error reading root chunk" );
		//ASSERT( rootChunk->debugDump(*this) );
	}
	
	return true;
}

// creates or append to an entry
static Return __db_append(DbFileBackend& db, const std::string& key, const std::string& value) {
	if(db.rootChunk == NULL) return "db append: db not initialized";
	ScopedLock lock(db.mutex);

	DbFile_ValueChunk chunk;
	ASSERT( db.rootChunk->getValue(db, key, chunk, /*createIfNotExist*/true, /*mustCreateNew*/false) );
	ASSERT( chunk.appendData(db, value) );
	return true;
}

static Return __db_set(DbFileBackend& db, const std::string& key, const std::string& value) {
	if(db.rootChunk == NULL) return "db set: db not initialized";
	ScopedLock lock(db.mutex);

	DbFile_ValueChunk chunk;
	ASSERT( db.rootChunk->getValue(db, key, chunk, /*createIfNotExist*/true, /*mustCreateNew*/false) );
	ASSERT( chunk.overwriteData(db, value) );
	return true;
}

static Return __db_get(DbFileBackend& db, const std::string& key, /*out*/ std::string& value) {
	if(db.rootChunk == NULL) return "db get: db not initialized";
	ScopedLock lock(db.mutex);

	DbFile_ValueChunk chunk;
	ASSERT( db.rootChunk->getValue(db, key, chunk, /*createIfNotExist*/false, /*mustCreateNew*/false) );
	ASSERT( chunk.getData(db, value) );
	return true;
}

// adds. if it exists, it fails
static Return __db_add(DbFileBackend& db, const std::string& key, const std::string& value) {
	if(db.rootChunk == NULL) return "db add: db not initialized";
	ScopedLock lock(db.mutex);

	DbFile_ValueChunk chunk;
	ASSERT( db.rootChunk->getValue(db, key, chunk, /*createIfNotExist*/true, /*mustCreateNew*/true) );
	ASSERT( chunk.overwriteData(db, value) );
	return true;
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
		Return r = __db_add(db, key, content);
		if(r) {
			id = newId;
			return true;
		}
		//cout << "__saveNewDbEntry: " << r.errmsg << endl;
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

