/* String utils
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#include "StringUtils.h"
#include <cstdio>

std::string hexString(char c) {
	char buf[3];
	sprintf(buf, "%02X", (int)(unsigned char)c);
	return buf;
}

std::string hexString(const std::string& rawData) {
	std::string ret;
    for(size_t i = 0; i < rawData.size(); ++i)
		ret += hexString(rawData[i]);
	return ret;
}

size_t findLastPathSep(const std::string& path) {
	size_t slash = path.rfind('\\');
	size_t slash2 = path.rfind('/');
	if(slash == std::string::npos)
		slash = slash2;
	else if(slash2 != std::string::npos)
		slash = std::max(slash, slash2);
	return slash;
}

std::string baseFilename(const std::string& filename) {
	size_t p = findLastPathSep(filename);
	if(p == std::string::npos) return filename;
	return filename.substr(p+1);
}

std::string	dirName(const std::string& filename) {
	size_t p = findLastPathSep(filename);
	if(p == std::string::npos) return "";
	return filename.substr(0, p);
}
