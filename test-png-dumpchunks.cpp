/* demo for PNG chunk reading
 * by Albert Zeyer, 2011
 * code under LGPL
 */

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
	if(Return r = png_read_sig(f))
		cout << "* signature: OK" << endl;
	else {	
		cout << "* signature: " << r.errmsg << endl;
		return 1;
	}
	
	while(!feof(f) && !ferror(f)) {
		PngChunk chunk;
		Return r = png_read_chunk(f, chunk);
		if(!r) {
			cout << "* chunk: " << r.errmsg << endl;
			return 1;
		}
		cout << "* chunk: " << chunk.type << ", len=" << chunk.data.size() << endl;
	}
	
	return 0;
}
