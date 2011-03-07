/* FUSE access to DB
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "Db.h"
#include "DbDefBackend.h"
#include "Utils.h"
#include "DbPng.h"

#include <string>
#include <errno.h>
#include <fcntl.h>
#include <cstring>

#define _FILE_OFFSET_BITS 64
#define FUSE_USE_VERSION  26
#include <fuse.h>

#include <iostream>
using namespace std;

static DbIntf* db = NULL;

static int db_getattr(const char *path, struct stat *stbuf) {
    memset(stbuf, 0, sizeof(struct stat));
	
	if(*path == '/') ++path; // skip '/' at the beginning
	if(*path == '\0') {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 3;
	}
	else {
		std::string filename = path;
		std::string basename = baseFilename(filename);
		std::string dirname = dirName(filename);
		
		std::list<DbDirEntry> dirList;
		if(!db->getDir(dirList, dirname))
			return -ENOENT;
		
		for(std::list<DbDirEntry>::iterator i = dirList.begin(); i != dirList.end(); ++i)
			if(i->name == basename) {
				stbuf->st_mode = i->mode;
				stbuf->st_nlink = 1;
				stbuf->st_size = i->size;
				break;
			}
	}
	
	if(stbuf->st_mode == 0)
		return -ENOENT;
	
	return 0;
}

static int db_open(const char *path, struct fuse_file_info *fi) {
	if(*path == '/') ++path; // skip '/' at the beginning

	DbEntryId id;
	if(!db->getFileRef(id, path))
		return -ENOENT;
		
    if ((fi->flags & O_ACCMODE) != O_RDONLY) /* Only reading allowed. */
        return -EACCES;
	
    return 0;
}

static int db_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
              off_t offset, struct fuse_file_info *fi) {
	if(*path == '/') ++path; // skip '/' at the beginning

	std::list<DbDirEntry> dirList;
	if(!db->getDir(dirList, path))
		return -ENOENT;
	
    filler(buf, ".", NULL, 0);           /* Current directory (.)  */
    filler(buf, "..", NULL, 0);          /* Parent directory (..)  */

	for(std::list<DbDirEntry>::iterator i = dirList.begin(); i != dirList.end(); ++i) {
		struct stat stbuf;
		memset(&stbuf, 0, sizeof(struct stat));
		stbuf.st_mode = i->mode;
		stbuf.st_nlink = 1;
		stbuf.st_size = i->size;
		filler(buf, i->name.c_str(), &stbuf, 0);
	}
	
    return 0;
}

static std::string lastReadFileName;
static std::string lastReadFileContent;

static int __return_content(char *buf, size_t size, off_t offset) {
	const std::string& content = lastReadFileContent;
	
	if(offset >= content.size()) // Reading behind the content.
		return 0;
		
    if (offset + size > content.size()) // Trim the read to the file size.
        size = content.size() - offset;
	
	memcpy(buf, &content[offset], size); // Provide the content.	
    return size;
}

static int db_read(const char *path, char *buf, size_t size, off_t offset,
           struct fuse_file_info *fi) {
	if(*path == '/') ++path; // skip '/' at the beginning
	if(*path == '\0') return -EISDIR; // this is the root-dir, not a file
	
	std::string filename = path;
	if(lastReadFileName == filename)
		return __return_content(buf, size, offset);
	
	DbEntryId fileEntryId;
	if(!db->getFileRef(fileEntryId, filename))
		return -ENOENT;

	// fileEntryId.empty() is allowed and means we have an empty file
	if(fileEntryId.empty()) return 0;

	lastReadFileName = filename;
	lastReadFileContent = "";
	struct WriteCallback : WriteCallbackIntf {
		Return write(const char* d, size_t s) {
			lastReadFileContent += std::string(d, s);
			return true;
		}
	} writer;
	DbPngEntryReader dbPngReader(&writer, db, fileEntryId);
	while(dbPngReader)
		if(!dbPngReader.next())
			return -EIO; // should not happen. thus return an IO error
	
	return __return_content(buf, size, offset);
}

int main(int argc, char **argv) {
	DbDefBackend dbInst;
	db = &dbInst;
	db->setReadOnly(true);
	Return r = db->init();
	if(!r) {
		cerr << "error: failed to init DB: " << r.errmsg << endl;
		return 1;
	}
	
	struct fuse_operations ops;
	memset(&ops, 0, sizeof(ops));
	ops.getattr = db_getattr;	/* To provide size, permissions, etc. */
	ops.open = db_open;			/* To enforce read-only access.       */
	ops.read = db_read;			/* To provide file content.           */
	ops.readdir = db_readdir;	/* To provide directory listing.      */
    return fuse_main(argc, argv, &ops, NULL);
}
