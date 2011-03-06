/* PNG data <-> DB interface
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "Db.h"
#include "DbPng.h"
#include "StringUtils.h"
#include "Sha1.h"

#include <sys/mman.h>
#include <iostream>
using namespace std;

#define PngBlockSize 64

Return DbPngEntryWriter::next() {
	ASSERT( reader.read() );

	while(true) {
		bool isFinal = false;
		if(reader.chunks.size() > 0) {
			DbEntry entry;
			entry.data += (char)DbEntryType_PngChunk;
			entry.data += reader.chunks.front().type;
			entry.data += reader.chunks.front().data;
			reader.chunks.pop_front();
			entry.prepare();
			
			DbEntryId id;
			ASSERT( db->push(id, entry) );
			contentEntries.push_back(id);
			continue;
		}
		else if(reader.scanlines.size() > 0) {
			if(reader.scanlines.size() >= PngBlockSize || reader.hasFinishedReading) {
				size_t scanlineWidth = reader.scanlines.front().size();
				uint8_t blockHeight = 0;
				for(std::list<std::string>::iterator it = reader.scanlines.begin();
					it != reader.scanlines.end() && blockHeight < PngBlockSize;
					++it) {
					if(it->size() != scanlineWidth) break;
					++blockHeight;
				}
				
				for(size_t x = 0; x < scanlineWidth; x += PngBlockSize) {
					DbEntry entry;
					entry.data += (char)DbEntryType_PngBlock;
					entry.data += rawString<uint16_t>( x / PngBlockSize );
					if(x == 0) entry.data += rawString<uint8_t>( blockHeight );
					size_t i = 0;
					for(std::list<std::string>::iterator it = reader.scanlines.begin(); i < blockHeight; ++it, ++i)
						entry.data += it->substr(x, PngBlockSize);
					entry.prepare();
					
					DbEntryId id;
					ASSERT( db->push(id, entry) );
					contentEntries.push_back(id);
				}
				
				for(size_t i = 0; i < blockHeight; ++i)
					reader.scanlines.pop_front();
				continue;				
			}
			else
				break;
		}
		else if(reader.hasFinishedReading) {
			DbEntry entry;
			entry.data += (char)DbEntryType_PngContentList;
			for(std::list<DbEntryId>::iterator i = contentEntries.begin(); i != contentEntries.end(); ++i) {
				entry.data += *i;
				entry.data += '\0';
			}
			entry.prepare();
			DbEntryId id;
			ASSERT( db->push(id, entry) );
			contentId = id;
			break;
		}
		
		break;
	}
	
	return true;
}
