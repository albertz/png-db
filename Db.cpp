/* simple DB interface
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef Z_CompressionLevel
#define Z_CompressionLevel 9
#endif

#include "Db.h"
#include "Sha1.h"
#include "StringUtils.h"
#include "FileUtils.h"
#include <cassert>
#include <zlib.h>
#include <cstdlib>

#include <iostream>
using namespace std;

void DbEntry::calcSha1() {
	sha1 = calc_sha1(data);
}

void DbEntry::compress() {
	compressed = "";
	
	z_stream stream;
	stream.zalloc = Z_NULL;
	stream.zfree = Z_NULL;
	stream.opaque = Z_NULL;
	deflateInit(&stream, Z_CompressionLevel);
	
	stream.avail_in = data.size();
	stream.next_in = (unsigned char*) &data[0];
	while(true) {
		char outputData[1024*128];
		stream.avail_out = sizeof(outputData);
		stream.next_out = (unsigned char*) outputData;
		int ret = deflate(&stream, Z_NO_FLUSH);
		switch(ret) {
			case Z_OK: break;
			case Z_STREAM_END: break;
			// these cases should not happen. but check anyway
			case Z_STREAM_ERROR: assert(false); return;
			default: assert(false); return;
		}
		size_t out_size = sizeof(outputData) - stream.avail_out;
		if(out_size == 0) break;
		compressed += std::string(outputData, out_size);
	}
}

Return DbEntry::uncompress() {
	data = "";
	
	z_stream stream;
	stream.zalloc = Z_NULL;
	stream.zfree = Z_NULL;
	stream.opaque = Z_NULL;
	inflateInit(&stream);
	
	bool gotStreamEnd = false;
	stream.avail_in = compressed.size();
	stream.next_in = (unsigned char*) &compressed[0];
	while(true) {
		char outputData[1024*128];
		stream.avail_out = sizeof(outputData);
		stream.next_out = (unsigned char*) outputData;
		int ret = inflate(&stream, Z_NO_FLUSH);
		switch(ret) {
			case Z_STREAM_ERROR: return "zlib stream error / invalid compression level";
			case Z_NEED_DICT: return "zlib need dict error";
			case Z_DATA_ERROR: return "zlib data error";
			case Z_MEM_ERROR: return "zlib out-of-memory error";
			case Z_STREAM_END: gotStreamEnd = true;
		}
		size_t out_size = sizeof(outputData) - stream.avail_out;
		if(out_size == 0) break;
		data += std::string(outputData, out_size);
	}
	
	if(!gotStreamEnd)
		return "zlib stream incomplete";
	return true;
}

std::string filenameForDbEntryId(const DbEntryId& id) {
	assert(!id.empty());
	
	std::string ret;
	for(size_t i = 0; i < id.size() - 1; ++i)
		ret += hexString(id[i]) + "/";
	ret += hexString(id[id.size()-1]) + ".dat";
	return ret;
}

std::string dirnameForSha1Ref(const std::string& sha1) {
	assert(sha1.size() == SHA1_DIGEST_SIZE);

	std::string ret;
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
		int n1 = __hexnumFromChar(fn[i]);
		int n2 = __hexnumFromChar(fn[i+1]);
		if(n1 < 0 || n2 < 0) return "";
		id += (char)(unsigned char)(n1 * 16 + n2);
	}
	if(i >= fn.size()) return "";
	if(fn.substr(i) != ".ref") return "";
	return id;
}

static Return __openNewDbEntry(const std::string& baseDir, DbEntryId& id, FILE*& f) {
	bool createdDir = false;
	unsigned short triesNum = (id.size() <= 4) ? (2 << id.size()) : 64;
	for(unsigned short i = 0; i < triesNum; ++i) {
		char c = random();
		std::string filename = baseDir + "/" + filenameForDbEntryId(id + c);
		if(!createdDir) {
			ASSERT( createRecDir(filename, false) );
			createdDir = true;
		}
		f = fopen(filename.c_str(), "wbx");
		if(f) return true;
	}

	char c = random();
	id += c;
	return __openNewDbEntry(baseDir, id, f);
}

Return Db::push(/*out*/ DbEntryId& id, const DbEntry& entry) {
	if(!entry.haveSha1())
		return "DB push: entry SHA1 not calculated";
	if(!entry.haveCompressed())
		return "DB push: entry compression not calculated";
	
	// search for existing entry
	std::string sha1refdir = dirnameForSha1Ref(entry.sha1);
	for(DirIter dir(baseDir + "/" + sha1refdir); dir; dir.next()) {
		DbEntryId otherId = __entryIdFromSha1RefFilename(dir.filename);
		cout << "direntry: " << dir.filename << ", entry=" << otherId << endl;
		if(otherId != "") {
			DbEntry otherEntry;
			if(get(otherEntry, otherId)) {
				if(entry == otherEntry) {
					// found
					id = otherId;
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
	std::string sha1reffn = sha1refdir + "/" + hexString(id) + ".ref";
	f = fopen(sha1reffn.c_str(), "w");
	if(f == NULL)
		return "DB push: cannot create SHA1 ref: cannot create file '" + sha1reffn + "'";
	fclose(f);
	
	return true;
}

Return Db::get(/*out*/ DbEntry& entry, const DbEntryId& id) {
	std::string filename = baseDir + "/" + filenameForDbEntryId(id);
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
