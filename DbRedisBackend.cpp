/* Redis DB backend
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "DbRedisBackend.h"
#include "StringUtils.h"
#include "Utils.h"
#include <cstdio>

DbRedisBackend::DbRedisBackend(const std::string& _prefix, const std::string& ip, int port) {
	prefix = _prefix;
	redis = redisConnect(ip.c_str(), port);
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
		std::string key = prefix + "data." + hexString(newId);
		RedisReplyWrapper reply( redisCommand(redis, "SETNX %s %b", key.c_str(), &content[0], content.size()) );
		ASSERT( reply );
		if(reply.reply->type == REDIS_REPLY_INTEGER && reply.reply->integer == 1)
			return true;
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
	RedisReplyWrapper reply( redisCommand(redis, "SMEMBERS %s", sha1refkey.c_str()) );
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
	reply = redisCommand(redis, "SADD %s %b", sha1refkey.c_str(), &id[0], id.size());
	ASSERT( reply );
	
	stats.pushNew++;
	return true;
}

Return DbRedisBackend::get(/*out*/ DbEntry& entry, const DbEntryId& id) {
	if(redis == NULL)
		return "DB get: Redis connection not initialized";

	std::string key = prefix + "data." + hexString(id);
	RedisReplyWrapper reply( redisCommand(redis, "GET %s", key.c_str()) );
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
