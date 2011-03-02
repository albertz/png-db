#include "Png.h"

/* Table of CRCs of all 8-bit messages. */
unsigned long crc_table[256];

/* Flag: has the table been computed? Initially false. */
int crc_table_computed = 0;

/* Make the table for a fast CRC. */
void make_crc_table(void)
{
	unsigned long c;
	int n, k;
	
	for (n = 0; n < 256; n++) {
		c = (unsigned long) n;
		for (k = 0; k < 8; k++) {
			if (c & 1)
				c = 0xedb88320L ^ (c >> 1);
			else
				c = c >> 1;
		}
		crc_table[n] = c;
	}
	crc_table_computed = 1;
}

#include <iostream>
using namespace std;

int main() {
	make_crc_table();
	
	cout << "{" << endl;
	for(int i = 0; i < 256; ++i) {
		cout << crc_table[i] << ", ";
		if(i % 50 == 0) cout << endl;
	}
	cout << "}" << endl;
}
