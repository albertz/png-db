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
	Return(const Return& r, const std::string& extmsg) : success(false) {
		if(r) errmsg = extmsg;
		else errmsg = extmsg + ": " + r.errmsg;
	}
	operator bool() const { return success; }
};

#define ASSERT(x) { Return ___r = (x); if(!___r) return ___r; }
#define ASSERT_EXT(x, msg) { Return ___r = (x); if(!___r) return Return(___r, msg); }

#endif
