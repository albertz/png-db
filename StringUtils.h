/* String utils
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef __AZ__STRINGUTILS_H__
#define __AZ__STRINGUTILS_H__

#include <string>
#include <list>

std::string hexString(char c);
std::string hexString(const std::string& rawData);

struct Matcher {
	struct ExprPart {
		std::string content;
	};
	std::list<ExprPart> expressionParts;
	Matcher(const std::string& expression);
	bool match(const std::string& s);
};

#endif
