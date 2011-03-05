/* String utils
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "StringUtils.h"
#include <cstdio>

std::string hexString(const std::string& rawData) {
	std::string ret;
    for(size_t i = 0; i < rawData.size(); ++i) {
		char buf[3];
        sprintf(buf, "%02X", rawData[i]);
		ret += buf;
    }
	return ret;
}
