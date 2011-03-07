/* Redis DB backend
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef __AZ__DBREDISBACKEND_H__
#define __AZ__DBREDISBACKEND_H__

#include "Db.h"
#include "credis.h"

struct DbRedisBackend : DbIntf {
	std::string prefix;
	REDIS redis;
	
	DbRedisBackend(const std::string& prefix = "db.", const std::string& host = "localhost", int port = 6379, int timeout = 2000);
	~DbRedisBackend();
	Return push(/*out*/ DbEntryId& id, const DbEntry& entry);
	Return get(/*out*/ DbEntry& entry, const DbEntryId& id);
};

#endif
