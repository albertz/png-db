/* String utils
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef __AZ__STRINGUTILS_H__
#define __AZ__STRINGUTILS_H__

#include <string>
#include <list>
#include "Endianess.h"

std::string hexString(char c);
std::string hexString(const std::string& rawData);

template<typename T>
std::string rawString(T val) {
	BEndianSwap(val);
	return std::string((char*)&val, sizeof(T));
}

template<typename T>
T valueFromRaw(const char* s) {
	T val = *(const T*)s;
	BEndianSwap(val);
	return val;
}


#endif
