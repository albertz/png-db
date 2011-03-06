/* String utils
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "StringUtils.h"
#include <cstdio>

std::string hexString(char c) {
	char buf[3];
	sprintf(buf, "%02X", c);
	return buf;
}

std::string hexString(const std::string& rawData) {
	std::string ret;
    for(size_t i = 0; i < rawData.size(); ++i)
		ret += hexString(rawData[i]);
	return ret;
}
