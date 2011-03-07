/* filesystem DB backend
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "DbFsBackend.h"
#include "Sha1.h"
#include "StringUtils.h"
#include "FileUtils.h"
#include <cassert>
#include <zlib.h>
#include <cstdlib>

#include <iostream>
using namespace std;

static std::string __filenameForDbEntryId(const DbEntryId& id) {
	assert(!id.empty());
	
	std::string ret = "data/";
	for(size_t i = 0; i < id.size() - 1; ++i)
		ret += hexString(id[i]) + "/";
	ret += hexString(id[id.size()-1]) + ".dat";
	return ret;
}

static std::string __dirnameForSha1Ref(const std::string& sha1) {
	assert(sha1.size() == SHA1_DIGEST_SIZE);
	
	std::string ret = "sha1refs/";
	for(size_t i = 0; i < sha1.size(); ++i) {
		if(i % 5 == 0 && i > 0) ret += "/";
		ret += hexString(sha1[i]);
	}
	return ret;
}

static int __hexnumFromChar(char c) {
	if(c >= '0' && c <= '9') return c - '0';
	if(c >= 'A' && c <= 'F') return 10 + c - 'A';
	if(c >= 'a' && c <= 'f') return 10 + c - 'a';
	return -1;
}

static DbEntryId __entryIdFromSha1RefFilename(const std::string& fn) {
	DbEntryId id;
	size_t i = 0;
	for(; i + 1 < fn.size(); i += 2) {
		if(fn.substr(i) == ".ref") return id;
		int n1 = __hexnumFromChar(fn[i]);
		int n2 = __hexnumFromChar(fn[i+1]);
		if(n1 < 0 || n2 < 0) return "";
		id += (char)(unsigned char)(n1 * 16 + n2);
	}
	return "";
}

static Return __openNewDbEntry(const std::string& baseDir, DbEntryId& id, FILE*& f) {
	bool createdDir = false;
	unsigned short triesNum = (id.size() <= 4) ? (2 << id.size()) : 64;
	for(unsigned short i = 0; i < triesNum; ++i) {
		DbEntryId newId = id;
		newId += (char)random();
		std::string filename = baseDir + "/" + __filenameForDbEntryId(newId);
		if(!createdDir) {
			ASSERT( createRecDir(filename, false) );
			createdDir = true;
		}
		f = fopen(filename.c_str(), "wbx");
		if(f) {
			id = newId;
			return true;
		}
	}
	
	id += (char)random();
	return __openNewDbEntry(baseDir, id, f);
}

Return DbFsBackend::push(/*out*/ DbEntryId& id, const DbEntry& entry) {
	if(!entry.haveSha1())
		return "DB push: entry SHA1 not calculated";
	if(!entry.haveCompressed())
		return "DB push: entry compression not calculated";
	
	// search for existing entry
	std::string sha1refdir = __dirnameForSha1Ref(entry.sha1);
	for(DirIter dir(baseDir + "/" + sha1refdir); dir; dir.next()) {
		DbEntryId otherId = __entryIdFromSha1RefFilename(dir.filename);
		if(otherId != "") {
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
	}
	
	// write DB entry
	id = "";
	FILE* f = NULL;
	ASSERT( __openNewDbEntry(baseDir, id, f) );
	{
		Return r = fwrite_all(f, entry.compressed);
		fclose(f);
		if(!r) return r;
	}
	
	// create sha1 ref
	std::string sha1reffn = baseDir + "/" + sha1refdir + "/" + hexString(id) + ".ref";
	ASSERT( createRecDir(sha1reffn, false) );
	f = fopen(sha1reffn.c_str(), "w");
	if(f == NULL)
		return "DB push: cannot create SHA1 ref: cannot create file '" + sha1reffn + "'";
	fclose(f);
	
	stats.pushNew++;
	return true;
}

Return DbFsBackend::get(/*out*/ DbEntry& entry, const DbEntryId& id) {
	std::string filename = baseDir + "/" + __filenameForDbEntryId(id);
	FILE* f = fopen(filename.c_str(), "rb");
	if(f == NULL)
		return "Db::get: cannot open file '" + filename + "'";
	
	{
		Return r = fread_all(f, entry.compressed);
		fclose(f);
		if(!r) return r;
	}
	
	ASSERT( entry.uncompress() );
	entry.calcSha1();
	
	return true;
}
