/* calculate CRC
 * based on http://www.w3.org/TR/PNG/#D-CRCAppendix
 * by Albert Zeyer, 2011
 * code public domain
 */

#ifndef __AZ__CRC_H__
#define __AZ__CRC_H__

#include <string>

uint32_t update_crc(uint32_t crc, const char *buf, size_t len);
uint32_t calc_crc(const char *buf, size_t len);
uint32_t calc_crc(const std::string& s);
uint32_t calc_crc(const std::string& s1, const std::string& s2);

#endif
