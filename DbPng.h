/* PNG data <-> DB interface
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef __AZ__DBPNG_H__
#define __AZ__DBPNG_H__

#include "Png.h"
#include "Db.h"
#include <cstdio>

struct DbPngEntryIter {
	PngReader reader;
	DbEntry entry;
	
	DbPngEntryIter(FILE* f) : reader(f) { next(); /* set first entry */ }
	Return next();
	operator bool() const { return !entry.data.empty(); }
};

#endif
