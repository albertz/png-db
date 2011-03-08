/* Mutex class - small wrapper around pthread
 * by Albert Zeyer, 2011
 * code under LGPL
 */

#ifndef __AZ__MUTEX_H__
#define __AZ__MUTEX_H__

#include "Utils.h"
#include <pthread.h>

struct Mutex {
	pthread_mutex_t m;
	Mutex() { pthread_mutex_init(&m, NULL); }
	Mutex(const Mutex&) { pthread_mutex_init(&m, NULL); } // ignore copy constr
	Mutex& operator=(const Mutex&) {} // ignore assign
	~Mutex() { pthread_mutex_destroy(&m); }
	void lock() { pthread_mutex_lock(&m); }
	bool trylock() { if(pthread_mutex_trylock(&m) == 0) return true; return false; }
	void unlock() { pthread_mutex_unlock(&m); }
};

struct ScopedLock : DontCopyTag {
	Mutex& mutex;
	ScopedLock(Mutex& m) : mutex(m) { mutex.lock(); }
	~ScopedLock() { mutex.unlock(); }
};

struct ScopedTryLock : DontCopyTag {
	Mutex& mutex;
	bool islocked;
	ScopedTryLock(Mutex& m) : mutex(m), islocked(false) { islocked = mutex.trylock(); }
	~ScopedTryLock() { if(islocked) { mutex.unlock(); islocked = false; } }	
	operator bool() const { return islocked; }
};

#endif
