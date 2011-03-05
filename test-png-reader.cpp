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
	Return r = reader.read();
	if(r)
		cout << "success" << endl;
	else {	
		cout << "error: " << r.errmsg << endl;
		return 1;
	}
		
	return 0;
}
