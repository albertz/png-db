/* filesystem DB backend
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef __AZ__DBFSBACKEND_H__
#define __AZ__DBFSBACKEND_H__

#include "Db.h"

struct DbFsBackend : DbIntf {
	std::string baseDir;
	DbStats stats;
	
	DbFsBackend(const std::string& d = ".") : baseDir(d) {}
	Return push(/*out*/ DbEntryId& id, const DbEntry& entry);
	Return get(/*out*/ DbEntry& entry, const DbEntryId& id);
};

#endif
