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
	while(!reader.hasFinishedReading) {
		Return r = reader.read();
		if(!r) {
			cout << "error: " << r.errmsg << endl;
			return 1;
		}
		
		size_t s = 0;
		for(std::list<std::string>::iterator i = reader.dataStream.begin(); i != reader.dataStream.end(); ++i)
			s += i->size();
		cout << "at " << ftell(f) << ": got " << s << " uncompressed" << endl;		
	}
	
	cout << "success" << endl;
	return 0;
}
