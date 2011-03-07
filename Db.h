/* simple DB interface
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef __AZ__DB_H__
#define __AZ__DB_H__

#include "Return.h"
#include "StringUtils.h"
#include <string>
#include <cassert>
#include <stdint.h>
#include <sys/stat.h>

#define DbEntryType_PngContentList 1
#define DbEntryType_PngChunk 2
#define DbEntryType_PngBlock 3

struct DbEntry {
	std::string data;
	std::string sha1;
	std::string compressed;
	
	DbEntry() {}
	DbEntry(const std::string& d) : data(d) { prepare(); }
	bool haveSha1() const { return !sha1.empty(); }
	void calcSha1();
	bool haveCompressed() const { return !compressed.empty(); }
	void compress();
	Return uncompress();
	void prepare() { calcSha1(); compress(); }
	
	bool operator==(const DbEntry& other) const {
		assert(haveSha1());
		assert(haveCompressed());
		assert(other.haveSha1());
		assert(other.haveCompressed());
		if(sha1 != other.sha1) return false;
		// Note: If we would ensure that the compression algorithm always works exactly
		// the same way, we could just restrict the check on the compressed data.
		// But as we don't want to restrict ourself to this, we can't.
		if(compressed == other.compressed) return true;
		return data == other.data;
	}
};

typedef std::string DbEntryId; /* guaranteed to not contain \0 and to be not empty */

struct DbStats {
	size_t pushNew;
	size_t pushReuse;
	DbStats() : pushNew(0), pushReuse(0) {}
};

struct DbDirEntry {
	mode_t mode;
	std::string name;
	size_t size;
	DbDirEntry(mode_t m = 0, const std::string& fn = "", size_t s = 0) : mode(m), name(fn), size(s) {}
	std::string serialized() const { return rawString<uint16_t>(mode) + rawString<uint32_t>(size) + name; }
	static DbDirEntry FromSerialized(const std::string& raw) {
		if(raw.size() <= 6) return DbDirEntry();
		return DbDirEntry(valueFromRaw<uint16_t>(&raw[0]), raw.substr(6), valueFromRaw<uint32_t>(&raw[2]));
	}
	static DbDirEntry File(const std::string& fn, size_t s) { return DbDirEntry(S_IFREG | 0444, fn, s); }
	static DbDirEntry Dir(const std::string& fn) { return DbDirEntry(S_IFDIR | 0755, fn); }
};

struct DbIntf {
	DbStats stats;
	virtual Return setReadOnly(bool ro) { return "Db::setReadOnly: not implemented"; }
	virtual Return init() = 0;
	virtual Return push(/*out*/ DbEntryId& id, const DbEntry& entry) = 0;
	virtual Return get(/*out*/ DbEntry& entry, const DbEntryId& id) = 0;
	virtual Return pushToDir(const std::string& path, const DbDirEntry& dirEntry) {
		return "Db::pushToDir: not implemented";
	}
	virtual Return getDir(/*out*/ std::list<DbDirEntry>& dirList, const std::string& path) {
		return "Db::getDir: not implemented";
	}
	virtual Return setFileRef(/*can be empty*/ const DbEntryId& id, const std::string& path) {
		return "Db::setFileRef: not implemented";
	}
	virtual Return getFileRef(/*out (can be empty)*/ DbEntryId& id, const std::string& path) {
		return "Db::getFileRef: not implemented";
	}
};

#endif
