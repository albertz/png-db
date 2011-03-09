/* single raw file DB backend
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef __AZ__DBFILEBACKEND_H__
#define __AZ__DBFILEBACKEND_H__

#include "Db.h"

struct DbFile_TreeChunk;

struct DbFileBackend : DbIntf {
	FILE* file;
	DbFile_TreeChunk* rootChunk;
	size_t fileSize;
	std::string filename;
	bool readonly;
	
	DbFileBackend(const std::string& dbfilename = "db.pngdb", bool ro = false) : filename(dbfilename), readonly(ro) {}
	~DbFileBackend() { reset(); }
	void reset();
	Return setReadOnly(bool ro) { readonly = ro; return true; }
	Return init();
	Return push(/*out*/ DbEntryId& id, const DbEntry& entry);
	Return get(/*out*/ DbEntry& entry, const DbEntryId& id);
	Return pushToDir(const std::string& path, const DbDirEntry& dirEntry);
	Return getDir(/*out*/ std::list<DbDirEntry>& dirList, const std::string& path);
	Return setFileRef(/*can be empty*/ const DbEntryId& id, const std::string& path);
	Return getFileRef(/*out (can be empty)*/ DbEntryId& id, const std::string& path);
};

#endif
