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
	std::string host;
	int port;
	redisContext* redis;
	
	DbRedisBackend(const std::string& _prefix = "db.", const std::string& _host = "127.0.0.1", int _port = 6379)
	: prefix(_prefix), host(_host), port(_port), redis(NULL) {}
	~DbRedisBackend();
	Return init();
	Return push(/*out*/ DbEntryId& id, const DbEntry& entry);
	Return get(/*out*/ DbEntry& entry, const DbEntryId& id);
	Return pushToDir(const std::string& path, const DbDirEntry& dirEntry);
	Return getDir(/*out*/ std::list<DbDirEntry>& dirList, const std::string& path);
	Return setFileRef(/*can be empty*/ const DbEntryId& id, const std::string& path);
	Return getFileRef(/*out (can be empty)*/ DbEntryId& id, const std::string& path);
};

#endif
