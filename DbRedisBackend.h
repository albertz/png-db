/* Redis DB backend
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef __AZ__DBREDISBACKEND_H__
#define __AZ__DBREDISBACKEND_H__

#include "Db.h"
#include "hiredis/hiredis.h"

struct DbRedisBackend : DbIntf {
	std::string prefix;
	redisContext* redis;
	
	DbRedisBackend(const std::string& prefix = "db.", const std::string& ip = "127.0.0.1", int port = 6379);
	~DbRedisBackend();
	Return push(/*out*/ DbEntryId& id, const DbEntry& entry);
	Return get(/*out*/ DbEntry& entry, const DbEntryId& id);
};

#endif
