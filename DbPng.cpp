/* PNG data <-> DB interface
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "Db.h"
#include "DbPng.h"

Return DbPngEntryWriter::next() {
	ASSERT( reader.read() );

	while(true) {
		bool isFinal = false;
		DbEntry entry;
		if(reader.chunks.size() > 0) {
			entry.data += (char)DbEntryType_PngChunk;
			entry.data += reader.chunks.front().type;
			entry.data += reader.chunks.front().data;
			reader.chunks.pop_front();
		}
		else if(reader.scanlines.size() > 0) {
			entry.data += (char)DbEntryType_PngScanline;
			entry.data += reader.scanlines.front();
			reader.scanlines.pop_front();
		}
		else if(reader.hasFinishedReading) {
			entry.data += (char)DbEntryType_PngContentList;
			for(std::list<DbEntryId>::iterator i = contentEntries.begin(); i != contentEntries.end(); ++i) {
				entry.data += *i;
				entry.data += '\0';
			}
			isFinal = true;
		}
		
		if(entry.data.size() > 0) {
			entry.prepare();
			DbEntryId id;
			ASSERT( db->push(id, entry) );
			if(isFinal) {
				contentId = id;
				break;
			}
			else
				contentEntries.push_back(id);
		}
		else
			break;
	}
	
	return true;
}
