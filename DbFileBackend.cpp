/* single raw file DB backend
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "DbFileBackend.h"
#include "StringUtils.h"
#include "Utils.h"
#include "FileUtils.h"
#include "Crc.h"
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

struct DbFile_TreeChunk {
	DbFile_TreeChunk() : selfOffset(0) {}
	size_t selfOffset;
	
	static const short NUM_ENTRIES = 8;
	struct Entry {
		std::string key;
		enum { ET_None=0, ET_Subtree=1, ET_Value=2 } type;
		uint64_t ref;
		Entry() : type(ET_None), ref(0) {}
		bool operator<(const Entry& o) const { return key < o.key; }
	};
	Entry entries[NUM_ENTRIES];
	
	uint8_t typesBitfield() {
		uint8_t types = 0;		
		for(int i = 0; i < NUM_ENTRIES; ++i)
			if(entries[i].type == Entry::ET_Value)
				types |= uint8_t(1) << i;
		return types;
	}
	
	void setFromTypesBitfield(uint8_t types) {
		for(int i = 0; i < NUM_ENTRIES; ++i)
			if(entries[i].type == Entry::ET_Value) {
				if(types & (uint8_t(1) << i))
					entries[i].type = Entry::ET_Value;
				else
					entries[i].type = Entry::ET_Subtree;
			}
	}
	
	Return write(DbFileBackend& db) {
		ASSERT( __db_fseek(db, selfOffset) );
		std::string data;
		
		data += rawString<uint8_t>(typesBitfield());
		for(short i = 0; i < NUM_ENTRIES; ++i)
			data += rawString<uint64_t>(entries[i].ref);

		uint16_t offset = 0;
		for(short i = 0; i < NUM_ENTRIES; ++i) {
			if(entries[i].key.size() >= 255) return "entry key size too big";
			offset += entries[i].key.size();
			data += rawString<uint16_t>(offset);
		}
		
		for(short i = 0; i < NUM_ENTRIES; ++i)
			data += entries[i].key;

		data += rawString<uint32_t>(calc_crc(data));
		data = rawString<uint32_t>(data.size() - sizeof(uint32_t)) + data;
		
		return __db_fwrite(db, data);
	}
	
	Return read(DbFileBackend& db) {
		ASSERT( __db_ftell(db, selfOffset) );

		size_t len = 0;
		ASSERT( fread_bigendian<uint32_t>(db.file, len) );
		if(len < sizeof(uint8_t) + NUM_ENTRIES*(sizeof(uint64_t) + sizeof(uint16_t)))
			return "TreeChunk data too small";
		
		std::string data('\0', len);
		ASSERT( fread_bytes(db.file, &data[0], data.size()) );
		
		uint32_t crc = 0;
		ASSERT( fread_bigendian<uint32_t>(db.file, crc) );
		
		if(crc != calc_crc(data))
			return "CRC missmatch on TreeChunk read";
		
		uint32_t offset = 0;
		setFromTypesBitfield(valueFromRaw<uint8_t>(&data[offset]));
		offset += sizeof(uint8_t);

		for(short i = 0; i < NUM_ENTRIES; ++i) {
			entries[i].ref = valueFromRaw<uint64_t>(&data[offset]);
			offset += sizeof(uint64_t);
			if(entries[i].ref == 0)
				entries[i].type = Entry::ET_None;
		}
		
		uint16_t offsets[NUM_ENTRIES];
		for(short i = 0; i < NUM_ENTRIES; ++i) {
			offsets[i] = valueFromRaw<uint16_t>(&data[offset]);
			offset += sizeof(uint16_t);
		}
		
		for(short i = 0; i < NUM_ENTRIES; ++i) {
			uint16_t keysize = offsets[i];
			if(i > 0) {
				if(offsets[i] < offsets[i-1])
					return "TreeChunk key offset data inconsistent (offsets not in order)";
				keysize -= offsets[i-1];
			}
			entries[i].key = data.substr(offset, keysize);
			offset += keysize;
			if(offset > data.size())
				return "TreeChunk key offset data inconsistent (go beyond the scope)";
			if(entries[i].key.empty())
				entries[i].type = Entry::ET_None;
		}
		
		return true;
	}
};

struct DbFile_ValueChunk {
	DbFile_ValueChunk() : selfOffset(0) {}
	size_t selfOffset;
	std::string data;
	
	Return write(DbFileBackend& db) {
		ASSERT( __db_fseek(db, selfOffset) );
		ASSERT( __db_fwrite(db, rawString<uint32_t>(data.size())) );
		ASSERT( __db_fwrite(db, data) );
		ASSERT( __db_fwrite(db, rawString<uint32_t>(calc_crc(data))) );
		return true;
	}
	
	Return read(DbFileBackend& db) {
		ASSERT( __db_ftell(db, selfOffset) );

		size_t len = 0;
		ASSERT( fread_bigendian<uint32_t>(db.file, len) );
		
		data = std::string('\0', len);
		ASSERT( fread_bytes(db.file, &data[0], data.size()) );
		
		uint32_t crc = 0;
		ASSERT( fread_bigendian<uint32_t>(db.file, crc) );
		
		if(crc != calc_crc(data))
			return "CRC missmatch on ValueChunk read";
		
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
		ASSERT( __db_ftell(*this, rootChunk->selfOffset) );
		ASSERT( rootChunk->write(*this) );
	}
	else if(fileSize < sizeof(DbFile_Signature))
		return "DB file even too small for the signature";
	else {
		char tmp[sizeof(DbFile_Signature)];
		ASSERT( fread_bytes(file, tmp, sizeof(tmp)) );
		if(memcpy(tmp, DbFile_Signature, sizeof(tmp)) != 0)
			return "DB file signature wrong";
		
		ASSERT_EXT( rootChunk->read(*this), "error reading root chunk" );
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

