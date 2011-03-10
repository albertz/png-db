/* PNG data <-> DB interface
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "Db.h"
#include "DbPng.h"
#include "StringUtils.h"
#include "Sha1.h"
#include <vector>
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
			contentChunkEntries.push_back(id);
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
					contentDataEntries.push_back(id);
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
			for(std::list<DbEntryId>::iterator i = contentChunkEntries.begin(); i != contentChunkEntries.end(); ++i) {
				if(i->size() > 255)
					return "we have an ID with len > 255";
				entry.data += rawString<uint8_t>(i->size());
				entry.data += *i;
			}
			for(std::list<DbEntryId>::iterator i = contentDataEntries.begin(); i != contentDataEntries.end(); ++i) {
				if(i->size() > 255)
					return "we have an ID with len > 255";
				entry.data += rawString<uint8_t>(i->size());
				entry.data += *i;
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

static Return __readPngChunk(PngChunk& chunk, const std::string& data) {
	if(data.size() < 5)
		return "chunk entry too small";
	chunk.type = data.substr(1,4);
	chunk.data = data.substr(5);
	return true;
}

static Return __finishBlock(DbPngEntryReader& png) {
	if(png.blockList.blockHeight == 0 && png.blockList.blocks.size() == 0)
		// there was no previos unfinished block. but this is ok
		return true;

	std::vector<std::string> scanlines(png.blockList.blockHeight);
	size_t x = 0;
	for(std::list<std::string>::iterator block = png.blockList.blocks.begin(); block != png.blockList.blocks.end(); ++block) {
		size_t blockWidth = block->size() / png.blockList.blockHeight;
		for(short y = 0; y < png.blockList.blockHeight; ++y) {
			scanlines[y] += block->substr(y * blockWidth, blockWidth);
		}
		x += blockWidth;
	}

	for(short y = 0; y < png.blockList.blockHeight; ++y)
		png.writer.scanlines.push_back(scanlines[y]);
	
	png.blockList = DbPngEntryBlockList();
	return true;
}

static Return __readBlock(DbPngEntryReader& png, const std::string& data) {
	size_t offset = 1; // first was the DbEntry type
	if(data.size() < offset + sizeof(uint16_t))
		return "block entry too small (before reading x-offset)";
	uint32_t x = valueFromRaw<uint16_t>(&data[offset]) * PngBlockSize;
	offset += sizeof(uint16_t);
	
	if(x == 0) {
		// new block row start
		ASSERT( __finishBlock(png) );
		if(data.size() < offset + sizeof(uint8_t))
			return "block entry too small (before reading block height)";
		png.blockList.blockHeight = valueFromRaw<uint8_t>(&data[offset]);
		offset += sizeof(uint8_t);
		if(png.blockList.blockHeight == 0)
			return "block height invalid";
	}
	
	if(((data.size() - offset) % png.blockList.blockHeight) != 0)
		return "block entry raw size is not a multiple of blockHeight";		
	size_t blockWidth = (data.size() - offset) / png.blockList.blockHeight;
	
	png.blockList.scanlineWidth += blockWidth;
	png.blockList.blocks.push_back( data.substr(offset) );

	return true;
}

Return DbPngEntryReader::next() {
	if(!haveContentEntries) {
		DbEntry entry;
		ASSERT( db->get(entry, contentId) );
		if(entry.data.size() == 0)
			return "content entry list data is empty";
		if(entry.data[0] != DbEntryType_PngContentList)
			return "content entry list data is invalid";			
		size_t i = 1;
		while(i < entry.data.size()) {
			uint8_t size = entry.data[i];
			++i;
			if(i + size > entry.data.size())
				return "content entry list data is inconsistent";
			contentEntries.push_back( DbEntryId(entry.data.substr(i, size)) );
			i += size;
		}
		haveContentEntries = true;
		return true;
	}

	if(contentEntries.size() > 0) {
		DbEntry entry;
		ASSERT( db->get(entry, contentEntries.front()) );
		contentEntries.pop_front();
		if(entry.data.size() == 0)
			return "content entry data is empty";
		switch(entry.data[0]) {
			case DbEntryType_PngChunk: {
				PngChunk chunk;
				ASSERT( __readPngChunk(chunk, entry.data) );
				writer.chunks.push_back(chunk);
				break;
			}
			case DbEntryType_PngBlock: {
				writer.hasAllChunks = true; // there wont be any more PngChunk entries
				ASSERT( __readBlock(*this, entry.data) );
				break;
			}
			default:
				return "content entry data is invalid";
		}
	}
	if(contentEntries.size() == 0) {
		ASSERT( __finishBlock(*this) );
		writer.hasAllChunks = true;
		writer.hasAllScanlines = true;
	}
	
	ASSERT( writer.write() );
	
	return true;
}
