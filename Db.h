/* simple DB interface
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef __AZ__DB_H__
#define __AZ__DB_H__

#include "Return.h"
#include <string>
#include <cassert>

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
		return sha1 == other.sha1 && compressed == other.compressed;
	}
};

typedef std::string DbEntryId; /* guaranteed to not contain \0 and to be not empty */

struct DbStats {
	size_t pushNew;
	size_t pushReuse;
	DbStats() : pushNew(0), pushReuse(0) {}
};

struct DbIntf {
	DbStats stats;
	virtual Return push(/*out*/ DbEntryId& id, const DbEntry& entry) = 0;
	virtual Return get(/*out*/ DbEntry& entry, const DbEntryId& id) = 0;
};

#endif
