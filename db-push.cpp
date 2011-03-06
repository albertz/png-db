/* tool to push some entry to the DB
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "Png.h"
#include "Db.h"

#include <cstdio>
#include <iostream>
using namespace std;

int main(int argc, char** argv) {
	if(argc <= 1) {
		cerr << "please give me a filename" << endl;
		return 1;
	}
	
	FILE* f = fopen(argv[1], "r");
	if(f == NULL) {
		cerr << "error: cannot open " << argv[1] << endl;
		return 1;
	}
	PngReader reader(f);
	bool haveSeenHeader = false;
	while(!reader.hasFinishedReading) {
		Return r = reader.read();
		if(!r) {
			cerr << "error: " << r.errmsg << endl;
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
	}
	
	size_t s = 0;
	for(std::list<std::string>::iterator i = reader.scanlines.begin(); i != reader.scanlines.end(); ++i)
		s += i->size();
	cout << "uncompressed data stream size: " << s << endl;		
	cout << "number scanlines: " << reader.scanlines.size() << endl;
	
	cout << "success" << endl;
	return 0;
}
