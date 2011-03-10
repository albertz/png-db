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
	DbFile_TreeChunk() : selfOffset(0), parent(NULL) {}
	size_t selfOffset;
	DbFile_TreeChunk* parent;
	
	static const short NUM_ENTRIES = 8;
	struct Entry {
		static const size_t KEYSIZELIMIT = 4;
		enum { ET_None=0, ET_Subtree=1, ET_Value=2 } type;
		std::string keyPart;
		uint64_t ref;
		Entry() : type(ET_None), ref(0) {}
		bool operator<(const Entry& o) const { return std::make_pair(int(type),keyPart) < std::make_pair(int(o.type),o.keyPart); }
		bool operator==(const Entry& o) const { return type == o.type && keyPart == o.keyPart; }
	};
	Entry entries[NUM_ENTRIES];
	
	int countNonEmptyEntries() {
		int count = 0;
		for(short i = 0; i < NUM_ENTRIES; ++i)
			if(entries[i].type != Entry::ET_None)
				count++;
		return count;
	}
	
	int firstFreeEntryIndex() {
		for(short i = 0; i < NUM_ENTRIES; ++i) {
			if(entries[i].type == Entry::ET_None)
				return i;
		}
		return -1;
	}
	
	Return __splitIfNeeded(DbFileBackend& db) {
		if(firstFreeEntryIndex() >= 0) return true; // no need to split
		
		// we are full. we must split
		DbFile_TreeChunk newSub;
		// take the last half. because they are ordered, we don't need to resort them
		for(short i = NUM_ENTRIES/2; i < NUM_ENTRIES; ++i) {
			newSub.entries[i] = entries[i];
			entries[i] = Entry(); // reset
		}
		newSub.parent = this;
		newSub.selfOffset = db.fileSize;
		ASSERT( newSub.write(db) );
		
		// make ref. we know that we don't have such a keyPart yet because of how getValue() works
		short index = firstFreeEntryIndex();
		entries[index].type = Entry::ET_Subtree;
		entries[index].keyPart = "";
		entries[index].ref = newSub.selfOffset;
		
		// NOTE: Entries are unordered now. But we assume that we
		// call __insert right after which does a resort and a write,
		// so it doesn't matter.
		return true;
	}
	
	Return __insert(DbFileBackend& db, const Entry& entry) {
		short index = firstFreeEntryIndex();
		if(index < 0) return "we asserted that we have a free entry index";
		entries[index] = entry;
		std::sort(&entries[0], &entries[NUM_ENTRIES]);
		ASSERT( write(db) );
		return true;
	}
	
	short __subtreeWithKeyStart(const std::string& keyPart) {
		for(short i = 0; i < NUM_ENTRIES; ++i)
			if(entries[i].type == Entry::ET_Subtree && entries[i].keyPart.substr(0,keyPart.size()) == keyPart)
				return i;
		return -1;
	}
	
	short __bestKeySize(const std::string& keyPart) {
		// NOTE: We cannot make some key more short. We cannot make some key longer.
		// So the any way to stay safe is to use len=1.
		return 1;
		
		short keySize = 1;
		if(countNonEmptyEntries() <= 2) keySize = 4;
		else if(countNonEmptyEntries() <= 4) keySize = 2;

		while(true) { // we are expecting in this loop that there isn't a same key already
			short index = __subtreeWithKeyStart(keyPart.substr(0,keySize));
			if(index < 0) break;
			keySize = entries[index].keyPart.size();
		}
		
		return keySize;
	}
	
	Return __createNewHere(DbFileBackend& db, const std::string& keyPart, /*out*/DbFile_ValueChunk& value) {
		if(keyPart.size() > Entry::KEYSIZELIMIT) {
			DbFile_TreeChunk subtree;
			subtree.parent = this;
			subtree.selfOffset = db.fileSize;
			ASSERT( subtree.write(db) );			
			
			ASSERT( __splitIfNeeded(db) );
			Entry entry;
			entry.type = Entry::ET_Subtree;			
			entry.keyPart = keyPart.substr(0, __bestKeySize(keyPart));
			entry.ref = subtree.selfOffset;
			ASSERT( __insert(db, entry) );
			
			return subtree.__createNewHere(db, keyPart.substr(entry.keyPart.size()), value);
		}
		
		ASSERT( __splitIfNeeded(db) );
		Entry entry;
		entry.type = Entry::ET_Value;
		entry.keyPart = keyPart;
		entry.ref = value.selfOffset = db.fileSize;
		return __insert(db, entry);
	}
	
	Return getValue(DbFileBackend& db, const std::string& key, /*out*/DbFile_ValueChunk& value,
					bool createIfNotExist = true, bool mustCreateNew = false) {
		short lastTreeIndex = -1;
		for(short i = 0; i < NUM_ENTRIES; ++i) {
			if(key.substr(0,entries[i].keyPart.size()) == entries[i].keyPart) {
				switch(entries[i].type) {
					case Entry::ET_None: break;
					case Entry::ET_Value:
						if(key == entries[i].keyPart) {
							// found it!
							if(mustCreateNew) return "entry does already exist";
							ASSERT( value.read(db, entries[i].ref) );
							return true;
						}
						break;
					case Entry::ET_Subtree:
						lastTreeIndex = i;
						break;
				}
			}
		}

		if(lastTreeIndex < 0) {
			if(!createIfNotExist) return "entry not found";
			return __createNewHere(db, key, value);
		}
		
		// we must/should go down
		DbFile_TreeChunk subtree;
		subtree.parent = this;
		ASSERT( subtree.read(db, entries[lastTreeIndex].ref) );
		return subtree.getValue(db, key.substr(entries[lastTreeIndex].keyPart.size()), value, createIfNotExist, mustCreateNew);		
	}
	
	uint8_t typesBitfield() {
		uint8_t types = 0;		
		for(short i = 0; i < NUM_ENTRIES; ++i)
			if(entries[i].type == Entry::ET_Value)
				types |= uint8_t(1) << i;
		return types;
	}
	
	void setFromTypesBitfield(uint8_t types) {
		for(short i = 0; i < NUM_ENTRIES; ++i)
			if(types & (uint8_t(1) << i))
				entries[i].type = Entry::ET_Value;
			else
				entries[i].type = Entry::ET_Subtree;
	}
	
	Return write(DbFileBackend& db) {
		ASSERT( __db_fseek(db, selfOffset) );
		ASSERT( __db_fwrite(db, rawString<uint8_t>(ChunkType_Tree)) );

		std::string data;		
		data += rawString<uint8_t>(typesBitfield());
		for(short i = 0; i < NUM_ENTRIES; ++i)
			data += rawString<uint64_t>(entries[i].ref);
		
		for(short i = 0; i < NUM_ENTRIES; ++i) {
			if(entries[i].keyPart.size() > Entry::KEYSIZELIMIT) return "entry keyPart size too big";
			data += rawString<uint8_t>(entries[i].keyPart.size());
			data += entries[i].keyPart;
			data += std::string(Entry::KEYSIZELIMIT - entries[i].keyPart.size(), '\0');
		}
		
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
		if(len != /*types*/sizeof(uint8_t) + NUM_ENTRIES*(/*refs*/sizeof(uint64_t) + /*keysize*/sizeof(uint8_t) + /*key*/Entry::KEYSIZELIMIT))
			return "TreeChunk data size missmatch";
		
		std::string data(len, '\0');
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
		
		for(short i = 0; i < NUM_ENTRIES; ++i) {
			uint8_t keysize = valueFromRaw<uint8_t>(&data[offset]);
			offset += sizeof(uint8_t);
			if(keysize > Entry::KEYSIZELIMIT)
				return "TreeChunk key size invalid";
			entries[i].keyPart = data.substr(offset, keysize);
			offset += Entry::KEYSIZELIMIT;
			if(i > 0 && entries[i-1].type != Entry::ET_None) {
				if(entries[i-1] == entries[i])
					return "TreeChunk keys inconsistent (double entry)";
				if(!(entries[i-1] < entries[i]))
					return "TreeChunk keys inconsistent (not in order)";
			}
		}
		
		return true;
	}
	
	Return debugDump(DbFileBackend& db, bool recursive = true, const std::string& prefix = "") {
		cout << prefix << "TreeChunk @" << selfOffset << " {" << endl;
		for(short i = 0; i < NUM_ENTRIES; ++i) {
			cout << prefix << "  " << int(entries[i].type) << ":" << hexString(entries[i].keyPart) << ":" << entries[i].ref << endl;
			if(recursive && entries[i].type == Entry::ET_Subtree) {
				DbFile_TreeChunk subtree;
				subtree.parent = this;
				ASSERT( subtree.read(db, entries[i].ref) );
				ASSERT( subtree.debugDump(db, true, prefix + "  ") );
			}
		}
		cout << prefix << "}" << endl;
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

