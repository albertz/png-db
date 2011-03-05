#include "Png.h"

#include <cstdio>
#include <iostream>
using namespace std;

int main(int argc, char** argv) {
	if(argc <= 1) {
		cout << "please give me a filename" << endl;
		return 1;
	}
	
	FILE* f = fopen(argv[1], "r");
	PngReader reader(f);
	bool haveSeenHeader = false;
	while(!reader.hasFinishedReading) {
		Return r = reader.read();
		if(!r) {
			cout << "error: " << r.errmsg << endl;
			return 1;
		}
		
		if(!haveSeenHeader && reader.gotHeader) {
			cout << "header: width=" << reader.header.width << ", height=" << reader.header.height << endl;
			cout << "header: bitDepth=" << (int)reader.header.bitDepth << endl;
			cout << "header: colorType=" << (int)reader.header.colourType << endl;
			cout << "header: bytesPerPixel=" << (int)reader.header.bytesPerPixel() << endl;
			cout << "header: compressionMethod=" << (int)reader.header.compressionMethod << endl;
			cout << "header: filterMethod=" << (int)reader.header.filterMethod << endl;
			cout << "header: interlaceMethod=" << (int)reader.header.interlaceMethod << endl;
			haveSeenHeader = true;
		}
		
		size_t s = 0;
		for(std::list<std::string>::iterator i = reader.dataStream.begin(); i != reader.dataStream.end(); ++i)
			s += i->size();
		cout << "at " << ftell(f) << ": got " << s << " uncompressed" << endl;		
	}
	
	cout << "success" << endl;
	return 0;
}
