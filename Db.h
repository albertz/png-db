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
#define DbEntryType_PngScanline 3

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

std::string filenameForDbEntryId(const DbEntryId& id);
std::string dirnameForSha1Ref(const std::string& sha1); // asserts that sha1.size() == SHA1_DIGEST_SIZE

struct Db {
	std::string baseDir;
	
	Db(const std::string& d = ".") : baseDir(d) {}
	Return push(/*out*/ DbEntryId& id, const DbEntry& entry);
	Return get(/*out*/ DbEntry& entry, const DbEntryId& id);
};

#endif
