/* simple DB interface
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef __AZ__DB_H__
#define __AZ__DB_H__

#include "Return.h"
#include <string>

#define DbEntryType_PngContentList 1
#define DbEntryType_PngChunk 2
#define DbEntryType_PngScanline 3

struct DbEntry {
	std::string data;
	std::string sha1;
	std::string compressed;
	
	DbEntry() {}
	DbEntry(const std::string& d) : data(d) { calcSha1(); }
	bool haveSha1() const { return !sha1.empty(); }
	void calcSha1();
	bool haveCompressed() const { return !compressed.empty(); }
	void compress();
	void uncompress();
	void prepare() { calcSha1(); compress(); }
};

typedef std::string DbEntryId; /* guaranteed to not contain \0 and to be not empty */

std::string filenameForDbEntryId(const DbEntryId& id);
std::string dirnameForSha1Ref(const std::string& sha1); // asserts that sha1.size() == SHA1_DIGEST_SIZE
std::string filenameForSha1Ref(const std::string& sha1, const DbEntryId& id);

struct Db {
	std::string baseDir;
	
	Db() : baseDir(".") {}
	Return push(/*out*/ DbEntryId& id, const DbEntry& entry);
	Return get(/*out*/ DbEntry& entry, const DbEntryId& id);
};

#endif
