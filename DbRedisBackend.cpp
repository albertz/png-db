/* Redis DB backend
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "DbRedisBackend.h"
#include "StringUtils.h"
#include "Utils.h"
#include <cstdio>

#include <iostream>
using namespace std;

DbRedisBackend::DbRedisBackend(const std::string& _prefix, const std::string& host, int port) {
	prefix = _prefix;
	redis = redisConnect(host.c_str(), port);
}

DbRedisBackend::~DbRedisBackend() {
	if(redis != NULL) {
		redisFree(redis);
		redis = NULL;
	}
}

struct RedisReplyWrapper : DontCopyTag {
	redisReply* reply;
	RedisReplyWrapper(void* r = NULL) : reply(NULL) { (*this) = r; }
	~RedisReplyWrapper() { clear(); }
	RedisReplyWrapper& operator=(void* r) { clear(); reply = (redisReply*)r; }
	void clear() {
		if(reply != NULL) {
			freeReplyObject(reply);
			reply = NULL;
		}
	}
	operator Return() const {
		if(reply == NULL) return "Redis: no reply";
		if(reply->type == REDIS_REPLY_ERROR) return std::string() + "Redis: " + reply->str;
		return true;
	}
};

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

Return DbRedisBackend::push(/*out*/ DbEntryId& id, const DbEntry& entry) {
	if(!entry.haveSha1())
		return "DB push: entry SHA1 not calculated";
	if(!entry.haveCompressed())
		return "DB push: entry compression not calculated";
	if(redis == NULL)
		return "DB push: Redis connection not initialized";
	if(!(redis->flags & REDIS_CONNECTED))
		return "DB push: Redis not connected";		
	
	// search for existing entry
	std::string sha1refkey = prefix + "sha1ref." + entry.sha1;
	RedisReplyWrapper reply( redisCommand(redis, "SMEMBERS %b", &sha1refkey[0], sha1refkey.size()) );
	ASSERT( reply );
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
	
	// write DB entry
	id = "";
	ASSERT( __saveNewDbEntry(redis, prefix, id, entry.compressed) );
	
	// create sha1 ref
	reply = redisCommand(redis, "SADD %b %b", &sha1refkey[0], sha1refkey.size(), &id[0], id.size());
	ASSERT( reply );
	
	stats.pushNew++;
	return true;
}

Return DbRedisBackend::get(/*out*/ DbEntry& entry, const DbEntryId& id) {
	if(redis == NULL)
		return "DB get: Redis connection not initialized";
	if(!(redis->flags & REDIS_CONNECTED))
		return "DB get: Redis not connected";		

	std::string key = prefix + "data." + id;
	RedisReplyWrapper reply( redisCommand(redis, "GET %b", &key[0], key.size()) );
	ASSERT(reply);
	if(reply.reply->type == REDIS_REPLY_NIL)
		return "DB get: entry not found";
	if(reply.reply->type != REDIS_REPLY_STRING)
		return "DB get: Redis: invalid GET reply";
	entry.compressed = std::string(reply.reply->str, reply.reply->len);
	
	ASSERT( entry.uncompress() );
	entry.calcSha1();
	
	return true;
}

Return DbRedisBackend::pushToDir(const std::string& path, const DbDirEntry& dirEntry) {
	std::string key = prefix + "fs." + path;
	std::string dirEntryRaw = dirEntry.serialized();
	RedisReplyWrapper reply( redisCommand(redis, "SADD %b %b", &key[0], key.size(), &dirEntryRaw[0], dirEntryRaw.size()) );
	ASSERT( reply );
	return true;
}

Return DbRedisBackend::getDir(/*out*/ std::list<DbDirEntry>& dirList, const std::string& path) {
	std::string key = prefix + "fs." + path;
	RedisReplyWrapper reply( redisCommand(redis, "SMEMBERS %b", &key[0], key.size()) );
	ASSERT( reply );
	if(reply.reply->type != REDIS_REPLY_ARRAY)
		return "DB getDir: invalid SMEMBERS reply";
	
	for(int i = 0; i < reply.reply->elements; ++i) {
		std::string dirEntryRaw(reply.reply->element[i]->str, reply.reply->element[i]->len);
		dirList.push_back( DbDirEntry::FromSerialized(dirEntryRaw) );
	}

	return true;
}

Return DbRedisBackend::setFileRef(/*can be empty*/ const DbEntryId& id, const std::string& path) {
	std::string key = prefix + "fs." + path;
	RedisReplyWrapper reply( redisCommand(redis, "SET %b %b", &key[0], key.size(), &id[0], id.size()) );
	ASSERT( reply );
	return true;
}

Return DbRedisBackend::getFileRef(/*out (can be empty)*/ DbEntryId& id, const std::string& path) {
	std::string key = prefix + "fs." + path;
	RedisReplyWrapper reply( redisCommand(redis, "GET %b", &key[0], key.size()) );
	ASSERT( reply );
	if(reply.reply->type != REDIS_REPLY_STRING)
		return "DB getFileRef: invalid GET reply";	
	id = std::string(reply.reply->str, reply.reply->len);
	return true;
}

