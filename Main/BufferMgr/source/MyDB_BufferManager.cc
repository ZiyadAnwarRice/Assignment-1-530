#ifndef BUFFER_MGR_C
#define BUFFER_MGR_C

#include "MyDB_BufferManager.h"
#include <algorithm>
#include <climits>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>

using namespace std;

MyDB_Page::MyDB_Page(MyDB_TablePtr table, long id, bool pinnedPage, MyDB_BufferManager& bufferMgr) 
	: table(table), pageId(id), pinned(pinnedPage), bufferManager(bufferMgr) {
    dirty = false;
    buffered = false;
    buffer = nullptr;
    refCount = 0;
    timestamp = 0;
}

MyDB_Page::~MyDB_Page() {
	if(buffered && buffer != nullptr) {
		bufferManager.DeallocateBuffer(buffer);
	}
}

void* MyDB_Page::GetBytes() {
	if(!buffered){
		if(!bufferManager.HasFreeBuffer()){
			MyDB_PagePtr lruPage = bufferManager.FindLRUPage();
			if(lruPage == nullptr) {
				return nullptr;
			}
			bufferManager.EvictPage(lruPage);
		}
		buffer = bufferManager.AllocateBuffer();
		if(buffer != nullptr)
		{
			buffered = true;
			bufferManager.LoadPageFromDisk(shared_from_this());
		}
	}
	timestamp = bufferManager.getNextTimeStamp();
	return buffer;
}


void MyDB_Page::WroteBytes() {
	dirty = true;
}

void MyDB_Page::AddRef() {
	refCount++;
}

void MyDB_Page::RemoveRef() {
    refCount--;
    if (refCount == 0) {
        if (table == nullptr) {
            if (buffered) {
                bufferManager.EvictPage(shared_from_this());
            }
        } else if (pinned) {
            pinned = false;
        }
    }
}

MyDB_PageHandle MyDB_BufferManager :: getPage (MyDB_TablePtr table, long id) {
	pair<MyDB_TablePtr, long> key = make_pair(table, id);
	MyDB_PagePtr page;
	if(pageTableMap.find(key) != pageTableMap.end()) {
		page = pageTableMap[key];
	} else {
		page = make_shared<MyDB_Page>(table, id, false, *this);
		pageTableMap[key] = page;
	}
	page->SetTimestamp(TimeStamp++);
	return make_shared<MyDB_PageHandleBase>(page);
}


MyDB_PageHandle MyDB_BufferManager :: getPage () {
	pair<MyDB_TablePtr, long> key = make_pair(nullptr, nextTempPageID++);
	MyDB_PagePtr page = make_shared<MyDB_Page>(nullptr, key.second, false, *this);
	pageTableMap[key] = page;
	page->SetTimestamp(TimeStamp++);
	return make_shared<MyDB_PageHandleBase>(page);
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage (MyDB_TablePtr table, long id) {
	pair<MyDB_TablePtr, long> key = make_pair(table, id);
	MyDB_PagePtr page;
	if(pageTableMap.find(key) != pageTableMap.end()) {
		page = pageTableMap[key];
	} else {
		page = make_shared<MyDB_Page>(table, id, true, *this);
		pageTableMap[key] = page;
	}
	page->SetPinned(true);
	page->SetTimestamp(TimeStamp++);
	return make_shared<MyDB_PageHandleBase>(page);		
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage () {
	pair<MyDB_TablePtr, long> key = make_pair(nullptr, nextTempPageID++);
	MyDB_PagePtr page = make_shared<MyDB_Page>(nullptr, key.second, true, *this);
	pageTableMap[key] = page;
	page->SetTimestamp(TimeStamp++);
	return make_shared<MyDB_PageHandleBase>(page);	
}

void MyDB_BufferManager :: unpin (MyDB_PageHandle unpinPage) {
	if (unpinPage != nullptr)
		unpinPage->GetPage()->SetPinned(false);
}

void* MyDB_BufferManager:: AllocateBuffer() {
	for (size_t i = 0; i < numPages; i++) {
		if (!bufferTaken[i]) {
			bufferTaken[i] = true;
			return buffer[i];
		}
	}
	return nullptr;
}

void MyDB_BufferManager:: DeallocateBuffer(void* buf) {
	for (size_t i = 0; i < numPages; i++) {
		if (buffer[i] == buf) {
			bufferTaken[i] = false;
			break;
		}
	}
}

bool MyDB_BufferManager:: HasFreeBuffer() {
	for (size_t i = 0; i < numPages; i++) {
        if (!bufferTaken[i]) {
            return true;
        }
    }
    return false;
}

MyDB_PagePtr MyDB_BufferManager:: FindLRUPage() {
	MyDB_PagePtr lru = nullptr;
	long minTimeStamp = LONG_MAX;
	for (auto& entry : pageTableMap) {
		MyDB_PagePtr page = entry.second;
		if (!page->IsPinned() && page->IsBuffered() && page->GetTimestamp() < minTimeStamp) {
			minTimeStamp = page->GetTimestamp();
			lru = page;
		}
	}
	return lru;
}

void MyDB_BufferManager:: EvictPage(MyDB_PagePtr page) {
	if (page->IsDirty()) {
		WritePageToDisk(page);
	}
	DeallocateBuffer(page->GetBuffer());
	page->SetBuffer(nullptr);
	page->SetBuffered(false);
	page->SetBuffer(nullptr);
}

void MyDB_BufferManager:: LoadPageFromDisk(MyDB_PagePtr page) {
	if(page->GetBuffer() == nullptr) {
		return;
	}
	const char* file;
	if(page->GetTable() == nullptr) {
		file = tempFile.c_str();
	} else {
		file = page->GetTable()->getStorageLoc().c_str();
	}
	int fd = open(file, O_RDONLY | O_CREAT, 0666);
	if(fd >= 0){
		off_t offset = page->GetPageId() * pageSize;
		lseek(fd, offset, SEEK_SET);
		memset(page->GetBuffer(), 0, pageSize);
		read(fd, page->GetBuffer(), pageSize);
		close(fd);
	}
}

void MyDB_BufferManager:: WritePageToDisk(MyDB_PagePtr page) {
	if(page->GetBuffer() == nullptr)
		return;

	const char* file;
	if(page->GetTable() == nullptr) {
		file = tempFile.c_str();
	} else {
		file = page->GetTable()->getStorageLoc().c_str();
	}
	int fd = open(file, O_WRONLY | O_CREAT | O_FSYNC, 0666);
	if(fd >= 0){
		off_t offset = page->GetPageId() * pageSize;
		lseek(fd, offset, SEEK_SET);
		write(fd, page->GetBuffer(), pageSize);
		close(fd);
	}
	page->SetDirty(false);
}

MyDB_BufferManager :: MyDB_BufferManager (size_t pageSize, size_t numPages, string tempFile)
: pageSize(pageSize), numPages(numPages), tempFile(tempFile) {
	buffer.resize(numPages);
	bufferTaken.resize(numPages, false);
	for(size_t i = 0; i < numPages; i++) {
		buffer[i] = malloc(pageSize);
	}
	nextTempPageID = 0;
	TimeStamp = 0;
}

MyDB_BufferManager :: ~MyDB_BufferManager () {
	for (auto& entry: pageTableMap) {
		MyDB_PagePtr page = entry.second;
		if (page->IsBuffered() && page->IsDirty())
			WritePageToDisk(page);
	}
	for (void* buf : buffer) {
			free(buf);
	}
	unlink(tempFile.c_str());
}
	
#endif


