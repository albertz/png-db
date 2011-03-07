/* Redis DB backend
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "DbRedisBackend.h"
#include "StringUtils.h"
#include <cstdio>

DbRedisBackend::DbRedisBackend(const std::string& _prefix, const std::string& host, int port, int timeout) {
	prefix = _prefix;
	redis = credis_connect(host.c_str(), port, timeout);
}

DbRedisBackend::~DbRedisBackend() {
	if(redis != NULL) {
		credis_close(redis);
		redis = NULL;
	}
}

static Return __saveNewDbEntry(REDIS redis, const std::string& prefix, DbEntryId& id, const std::string& content) {
	unsigned short triesNum = (id.size() <= 4) ? (2 << id.size()) : 64;
	for(unsigned short i = 0; i < triesNum; ++i) {
		DbEntryId newId = id;
		newId += (char)random();
		std::string key = prefix + "data." + hexString(newId);
		//credis_setnx(redis, key.c_str(), content);
	}
	
	id += (char)random();
	return __saveNewDbEntry(redis, prefix, id, content);
}

Return DbRedisBackend::push(/*out*/ DbEntryId& id, const DbEntry& entry) {
	if(!entry.haveSha1())
		return "DB push: entry SHA1 not calculated";
	if(!entry.haveCompressed())
		return "DB push: entry compression not calculated";
	if(redis == NULL)
		return "DB push: Redis connection not initialized";
	
	// search for existing entry
	std::string sha1refkey = prefix + "sha1ref." + hexString(entry.sha1);
	char** sha1refs = NULL;
	int sha1refCount = credis_smembers(redis, sha1refkey.c_str(), &sha1refs);
	if(sha1refCount > 0) {
		for(int i = 0; i < sha1refCount; ++i) {
			DbEntryId otherId = sha1refs[i];
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
	ASSERT( __saveNewDbEntry(redis, prefix, id, entry.compressed) );
	
	// create sha1 ref
	//credis_sadd(redis, sha1refkey.c_str(), id);
	
	stats.pushNew++;
	return true;
}

Return DbRedisBackend::get(/*out*/ DbEntry& entry, const DbEntryId& id) {
	if(redis == NULL)
		return "DB get: Redis connection not initialized";

	std::string key = prefix + "data." + hexString(id);
	//credis_get(redis, key.c_str(), &entry.compressed);

	ASSERT( entry.uncompress() );
	entry.calcSha1();
	
	return true;
}
