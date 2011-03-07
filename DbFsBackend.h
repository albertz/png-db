/* filesystem DB backend
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef __AZ__DBFSBACKEND_H__
#define __AZ__DBFSBACKEND_H__

#include "Db.h"

struct DbFsBackend : DbIntf {
	std::string baseDir;
	
	DbFsBackend(const std::string& d = "db") : baseDir(d) {}
	Return init() { return true; }
	Return push(/*out*/ DbEntryId& id, const DbEntry& entry);
	Return get(/*out*/ DbEntry& entry, const DbEntryId& id);
};

#endif
