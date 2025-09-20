
#ifndef PAGE_HANDLE_C
#define PAGE_HANDLE_C

#include <memory>
#include "MyDB_PageHandle.h"
#include "MyDB_BufferManager.h"

void *MyDB_PageHandleBase :: getBytes () {
	if(page != nullptr)
		return page->GetBytes();
	return nullptr;
}

void MyDB_PageHandleBase :: wroteBytes () {
	if(page != nullptr)
		page->WroteBytes();
}

MyDB_PageHandleBase :: ~MyDB_PageHandleBase () {
	if(page != nullptr) {
		page->RemoveRef();
	}
}

MyDB_PageHandleBase :: MyDB_PageHandleBase (MyDB_PagePtr p) {
	page = p;
	if(page != nullptr) {
		page->AddRef();
	}
}

#endif

