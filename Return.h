/* Return value class
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef __AZ__RETURN_H__
#define __AZ__RETURN_H__

#include <string>

struct Return {
	bool success;
	std::string errmsg;
	
	Return(bool s = true) : success(s) {}
	Return(const std::string& errm) : success(false), errmsg(errm) {}
	operator bool() { return success; }
};

#define ASSERT(x) { Return ___r = (x); if(!___r) return ___r; }

#endif
