/* FUSE access to DB
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "Db.h"
#include "DbDefBackend.h"
#include "Utils.h"
#include "DbPng.h"
#include "Mutex.h"
#include "SmartPointer.h"

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

//#define DEBUG

#ifdef DEBUG
#define debugPrint(out, msg) { out << "**** " << (std::string() + msg) << " ****" << endl; }
#else
#define debugPrint(out, msg) {}
#endif

#define CHECK_RET(x, err_ret, err_msg) \
	{ Return ___r = (x); if(!___r) { debugPrint(cerr, err_msg + ": " + ___r.errmsg); return err_ret; } }

// Currently, the size we return here is not exactly correct (it is the size
// of the files when we pushed them to the DB but they will likely be somewhat
// different when you get them out).
// If this size is smaller then what we would return here, we get into trouble
// that many readers would skip the rest of the data and thus the PNG seems
// incomplete.
// So just add some random value to make sure we return a size which is bigger
// than the actual file we would return here.
#define SAFETY_ADDED_SIZE 1000000

static int db_getattr(const char *path, struct stat *stbuf) {
    memset(stbuf, 0, sizeof(struct stat));
	debugPrint(cout, "db_getattr: " + path);
	
	if(*path == '/') ++path; // skip '/' at the beginning
	if(*path == '\0') {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
	}
	else {
		std::string filename = path;
		std::string basename = baseFilename(filename);
		std::string dirname = dirName(filename);
		
		std::list<DbDirEntry> dirList;
		CHECK_RET(db->getDir(dirList, dirname), -ENOENT, "db_getattr: getDir failed");
		
		for(std::list<DbDirEntry>::iterator i = dirList.begin(); i != dirList.end(); ++i)
			if(i->name == basename) {
				stbuf->st_mode = i->mode;
				stbuf->st_nlink = 1;
				stbuf->st_size = i->size + /* to be sure */ SAFETY_ADDED_SIZE;
				break;
			}
	}
	
	CHECK_RET(stbuf->st_mode != 0, -ENOENT, "db_getattr: not found");
	return 0;
}

static int db_open(const char *path, struct fuse_file_info *fi) {
	if(*path == '/') ++path; // skip '/' at the beginning
	std::string filename = path;
	filename = dirName(filename) + "/" + baseFilename(filename);
	
	debugPrint(cout, "db_open: " + filename);

	DbEntryId id;
	CHECK_RET(db->getFileRef(id, filename), -ENOENT, "db_open: fileref not found");
		
	CHECK_RET((fi->flags & O_ACCMODE) == O_RDONLY, -EACCES, "db_open: only reading allowed");
	
    return 0;
}

static int db_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
              off_t offset, struct fuse_file_info *fi) {
	if(*path == '/') ++path; // skip '/' at the beginning
	
	std::list<DbDirEntry> dirList;
	CHECK_RET(db->getDir(dirList, path), -ENOENT, "db_readdir: getDir failed");
	
    filler(buf, ".", NULL, 0);           /* Current directory (.)  */
    filler(buf, "..", NULL, 0);          /* Parent directory (..)  */

	for(std::list<DbDirEntry>::iterator i = dirList.begin(); i != dirList.end(); ++i) {
		struct stat stbuf;
		memset(&stbuf, 0, sizeof(struct stat));
		stbuf.st_mode = i->mode;
		stbuf.st_nlink = 1;
		stbuf.st_size = i->size + /* to be sure */ SAFETY_ADDED_SIZE;
		filler(buf, i->name.c_str(), &stbuf, 0);
	}
	
    return 0;
}


struct FileContent : WriteCallbackIntf {
	Mutex mutex;
	DbEntryId fileEntryId;
	std::string data;
	bool finished;
	DbPngEntryReader dbPngReader;
	
	FileContent(const std::string& _fileEntryId)
	: fileEntryId(_fileEntryId), finished(false), dbPngReader(this, db, fileEntryId) {}
	
	Return write(const char* d, size_t s) {
		// we already locked the mutex here
		data.append(d, s);
		return true;
	}
	
	int read(char* buf, size_t size, off_t offset) {
		ScopedLock lock(mutex);
		
		while(!finished && offset + size > data.size()) {
			if(dbPngReader) {
				CHECK_RET(dbPngReader.next(), -EIO, "db_read: reading failed");
			} else
				finished = true;
		}
		
		if(offset >= data.size()) // Reading behind the content.
			return 0;
		
		if (offset + size > data.size()) // Trim the read to the file size.
			size = data.size() - offset;
		
		memcpy(buf, &data[offset], size); // Provide the content.	
		return size;
	}
};

struct FileCache {
	static const size_t NUM_ENTRIES = 20;
	Mutex mutex;
	typedef std::list< SmartPointer<FileContent> > CacheList;
	CacheList cache;
	
	SmartPointer<FileContent> getContent(const DbEntryId& fileEntryId) {
		ScopedLock lock(mutex);
		for(CacheList::iterator i = cache.begin(); i != cache.end(); ++i) {
			// i->fileEntryId can be accessed safely because we never write it again
			if((*i)->fileEntryId == fileEntryId) {
				// found it
				// now push to front of cacheList
				SmartPointer<FileContent> content = *i;
				cache.erase(i);
				cache.push_front(content);
				return content;
			}
		}
		
		// not found -> create new
		SmartPointer<FileContent> content = new FileContent(fileEntryId);
		cache.push_front(content);
		// pop old entries
		while(cache.size() > NUM_ENTRIES)
			cache.pop_back();
		return content;
	}
	
	int read(const DbEntryId& fileEntryId, char* buf, size_t size, off_t offset) {
		return getContent(fileEntryId)->read(buf, size, offset);
	}
};

static int db_read(const char *path, char *buf, size_t size, off_t offset,
           struct fuse_file_info *fi) {
	if(*path == '/') ++path; // skip '/' at the beginning
	if(*path == '\0') return -EISDIR; // this is the root-dir, not a file
	
	std::string filename = path;
	filename = dirName(filename) + "/" + baseFilename(filename);
	
	DbEntryId fileEntryId;
	CHECK_RET(db->getFileRef(fileEntryId, filename), -ENOENT, "db_read: getFileRef failed");

	// fileEntryId.empty() is allowed and means we have an empty file
	if(fileEntryId.empty()) return 0;

	static FileCache fileCache;
	return fileCache.read(fileEntryId, buf, size, offset);
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
