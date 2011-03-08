/*
	OpenLieroX
	code under LGPL
	27-12-2007 Albert Zeyer
*/

#ifndef __SMARTPOINTER_H__
#define __SMARTPOINTER_H__

#include <limits.h>
#include <cassert>
#include "Mutex.h"

// Default de-initialization action is to call operator delete, for each object type.
template < typename _Type >
void SmartPointer_ObjectDeinit( _Type * obj ) {
	delete obj;
}


/*
	standard smartpointer based on simple refcounting

	The refcounting is multithreading safe in this class,
	you can have copies of this object in different threads.
	Though it's not designed to operate with the same
	object in different threads. Also there is absolutly no
	thread safty on the pointer itself, you have to care
	about this yourself.
*/


template < typename _Type >
class SmartPointer {
public:
	typedef _Type value_type;
private:
	_Type* obj;
	int* refCount;
	Mutex* mutex;

	void init(_Type* newObj) {
		if( newObj == NULL )
			return;
		if(!mutex) {
			mutex = new Mutex();
			obj = newObj;
			refCount = new int(1);
		}
	}

	void reset() {
		if(mutex) {
			lock();
			(*refCount)--;
			if(*refCount == 0) {
				SmartPointer_ObjectDeinit( obj );
				delete refCount; // safe, because there is no other ref anymore
				obj = NULL;
				refCount = NULL;
				unlock();
				delete mutex; // safe because there is no other ref anymore
				mutex = NULL;
		 	} else
		 		unlock();
		}
		obj = NULL;
		refCount = NULL;
		mutex = NULL;
	}

	void incCounter() {
		assert(*refCount > 0 && *refCount < INT_MAX);
		(*refCount)++;
	}

	void lock() { mutex->lock(); }
	void unlock() { mutex->unlock(); }

public:
	SmartPointer() : obj(NULL), refCount(NULL), mutex(NULL) {}
	~SmartPointer() { reset(); }

	// Default copy constructor and operator=
	// If you specify any template<> params here these funcs will be silently ignored by compiler
	SmartPointer(const SmartPointer& pt) : obj(NULL), refCount(NULL), mutex(NULL) { operator=(pt); }
	SmartPointer& operator=(const SmartPointer& pt) {
		if(mutex == pt.mutex) return *this; // ignore this case
		reset();
		mutex = pt.mutex;
		if(mutex) {
			lock();
			obj = pt.obj; refCount = pt.refCount;
			incCounter();
			unlock();
		} else { obj = NULL; refCount = NULL; }
		return *this;
	}

	// WARNING: Be carefull, don't assing a pointer to different SmartPointer objects,
	// else they will get freed twice in the end. Always copy the SmartPointer itself.
	// In short: SmartPointer ptr(SomeObj); SmartPointer ptr1( ptr.get() ); // It's wrong, don't do that.
	SmartPointer(_Type* pt): obj(NULL), refCount(NULL), mutex(NULL) { operator=(pt); }
	SmartPointer& operator=(_Type* pt) {
		if(obj == pt) return *this; // ignore this case
		reset();
		init(pt);
		return *this;
	}

	_Type* get() const { return obj; }	// The smartpointer itself won't change when returning address of obj, so it's const.

	// HINT: no convenient cast functions in this class to avoid error-prone automatic casts
	// (which would lead to collisions!)
	// This operator is safe though.
	_Type * operator -> () const { return obj; };

	// refcount may be changed from another thread, though if refcount==1 or 0 it won't change
	int getRefCount() {
		int ret = 0;
		if(mutex) {
			lock();
			ret = *refCount;
			unlock(); // Here the other thread may change refcount, that's why it's approximate
		}
		return ret;
	}

	// Returns true only if the data is deleted (no other smartpointer used it), sets pointer to NULL then
	bool tryDeleteData() {
		if(mutex) {
			lock();
			if( *refCount == 1 )
			{
				unlock(); // Locks mutex again inside reset(), since we're only ones using data refcount cannot change from other thread
				reset();
				return true;	// Data deleted
			}
			unlock();
			return false; // Data not deleted
		}
		return true;	// Data was already deleted
	}

};

#endif

