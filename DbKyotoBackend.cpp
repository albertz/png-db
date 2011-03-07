/* KyotoCabinet DB backend
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "DbKyotoBackend.h"
#include "StringUtils.h"
#include "Utils.h"
#include <cstdio>

#include <iostream>
using namespace std;
using namespace kyotocabinet;

DbKyotoBackend::~DbKyotoBackend() {
}

Return DbKyotoBackend::init() {
	if(!db.open(filename, PolyDB::OWRITER | PolyDB::OCREATE))
		return std::string() + "failed to open KyotoCabinet DB: " + db.error().name();
	return true;
}

/*
static Return __saveNewDbEntry(redisContext* redis, const std::string& prefix, DbEntryId& id, const std::string& content) {
	unsigned short triesNum = (id.size() <= 4) ? (2 << id.size()) : 64;
	for(unsigned short i = 0; i < triesNum; ++i) {
		DbEntryId newId = id;
		newId += (char)random();
		std::string key = prefix + "data." + newId;
		RedisReplyWrapper reply( redisCommand(redis, "SETNX %b %b", &key[0], key.size(), &content[0], content.size()) );
		ASSERT( reply );
		if(reply.reply->type == REDIS_REPLY_INTEGER && reply.reply->integer == 1) {
			id = newId;
			return true;
		}
	}
	
	id += (char)random();
	return __saveNewDbEntry(redis, prefix, id, content);
}
 */

Return DbKyotoBackend::push(/*out*/ DbEntryId& id, const DbEntry& entry) {
	if(!entry.haveSha1())
		return "DB push: entry SHA1 not calculated";
	if(!entry.haveCompressed())
		return "DB push: entry compression not calculated";
	
	// search for existing entry
	std::string sha1refkey = "sha1ref." + entry.sha1;
/*	RedisReplyWrapper reply( redisCommand(redis, "SMEMBERS %b", &sha1refkey[0], sha1refkey.size()) );
	if(reply.reply->type == REDIS_REPLY_ARRAY)
		for(int i = 0; i < reply.reply->elements; ++i) {
			DbEntryId otherId = std::string(reply.reply->element[i]->str, reply.reply->element[i]->len);
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
*/	
	// write DB entry
	id = "";
//	ASSERT( __saveNewDbEntry(redis, prefix, id, entry.compressed) );
	
	// create sha1 ref
//	reply = redisCommand(redis, "SADD %b %b", &sha1refkey[0], sha1refkey.size(), &id[0], id.size());
//	ASSERT( reply );
	
	stats.pushNew++;
	return true;
}

Return DbKyotoBackend::get(/*out*/ DbEntry& entry, const DbEntryId& id) {
	std::string key = "data." + id;
/*	RedisReplyWrapper reply( redisCommand(redis, "GET %b", &key[0], key.size()) );
	if(reply.reply->type == REDIS_REPLY_NIL)
		return "DB get: entry not found";
	if(reply.reply->type != REDIS_REPLY_STRING)
		return "DB get: Redis: invalid GET reply";
	entry.compressed = std::string(reply.reply->str, reply.reply->len); */
	
	//ASSERT( entry.uncompress() );
	entry.calcSha1();
	
	return true;
}

Return DbKyotoBackend::pushToDir(const std::string& path, const DbDirEntry& dirEntry) {
	std::string key = "fs." + path;
	std::string dirEntryRaw = dirEntry.serialized();
//	RedisReplyWrapper reply( redisCommand(redis, "SADD %b %b", &key[0], key.size(), &dirEntryRaw[0], dirEntryRaw.size()) );
	return true;
}

Return DbKyotoBackend::getDir(/*out*/ std::list<DbDirEntry>& dirList, const std::string& path) {
	std::string key = "fs." + path;
/*	RedisReplyWrapper reply( redisCommand(redis, "SMEMBERS %b", &key[0], key.size()) );
	if(reply.reply->type != REDIS_REPLY_ARRAY)
		return "DB getDir: invalid SMEMBERS reply";
	
	for(int i = 0; i < reply.reply->elements; ++i) {
		std::string dirEntryRaw(reply.reply->element[i]->str, reply.reply->element[i]->len);
		dirList.push_back( DbDirEntry::FromSerialized(dirEntryRaw) );
	}*/

	return true;
}

Return DbKyotoBackend::setFileRef(/*can be empty*/ const DbEntryId& id, const std::string& path) {
	std::string key = "fs." + path;
//	RedisReplyWrapper reply( redisCommand(redis, "SET %b %b", &key[0], key.size(), &id[0], id.size()) );
	return true;
}

Return DbKyotoBackend::getFileRef(/*out (can be empty)*/ DbEntryId& id, const std::string& path) {
	std::string key = "fs." + path;
/*	RedisReplyWrapper reply( redisCommand(redis, "GET %b", &key[0], key.size()) );
	if(reply.reply->type != REDIS_REPLY_STRING)
		return "DB getFileRef: invalid GET reply";	
	id = std::string(reply.reply->str, reply.reply->len); */
	return true;
}
