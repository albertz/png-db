/* simple DB interface
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "Db.h"
#include "Sha1.h"
#include "StringUtils.h"
#include "FileUtils.h"
#include <cassert>

#include <iostream>
using namespace std;

void DbEntry::calcSha1() {
	sha1 = calc_sha1(data);
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
	
	std::string sha1refdir = dirnameForSha1Ref(entry.sha1);
	for(DirIter dir(baseDir + "/" + sha1refdir); dir; dir.next()) {
		cout << "direntry: " << dir.filename << endl;
	}
	
	return true;
}

Return Db::get(/*out*/ DbEntry& entry, const DbEntryId& id) {
	
	return true;
}
