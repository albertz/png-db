/*
 (from OpenLieroX)
 
 makros for endianess / swapping / conversions
 
 Code under LGPL
 by Albert Zeyer, Dark Charlie, 23-06-2007
 */

#ifndef __ENDIANSWAP_H__
#define __ENDIANSWAP_H__

#include <machine/endian.h>

#if !defined(BYTE_ORDER)
#	error BYTE_ORDER not defined
#endif
#if BYTE_ORDER == LITTLE_ENDIAN
#	define EndianSwap(x)		;
#	define BEndianSwap(x)		ByteSwap5(x);
#	define GetEndianSwapped(x)	(x)
#elif BYTEORDER == BIG_ENDIAN
#	define EndianSwap(x)		ByteSwap5(x);
#	define BEndianSwap(x)		;
#	define GetEndianSwapped(x)	(GetByteSwapped(x))
#else
#	error unknown ENDIAN type
#endif

#include <algorithm>
#include "StaticAssert.h"

template <int n>
void ByteSwap(unsigned char * b) {
	static_assert(n == 1 || n % 2 == 0, n_must_be_equal);
	for(int i = 0; i < n/2; ++i) {
		std::swap(b[i], b[n - i - 1]);
	}
}

template <typename T>
T GetByteSwapped(T b) {
	ByteSwap<sizeof(T)>(&b);
	return b;
}

template <typename T>
void ByteSwap5(T& x) {
	ByteSwap<sizeof(T)>((unsigned char*) &x);
}

#endif

