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

std::string filenameForSha1Ref(const std::string& sha1, const DbEntryId& id) {
	return dirnameForSha1Ref(sha1) + "/" + hexString(id) + ".ref";
}

Return Db::push(/*out*/ DbEntryId& id, const DbEntry& entry) {
	if(!entry.haveSha1())
		return "DB push: entry SHA1 not calculated";
	if(!entry.haveCompressed())
		return "DB push: entry compression not calculated";
	
	std::string sha1refdir = dirnameForSha1Ref(entry.sha1);
	for(DirIter dir(baseDir + "/" + sha1refdir); dir; dir.next()) {
		cout << "direntry: " << dir.filename << endl;
	}
	
	return true;
}

Return Db::get(/*out*/ DbEntry& entry, const DbEntryId& id) {
	
	return true;
}
