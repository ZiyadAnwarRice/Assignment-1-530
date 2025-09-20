
#ifndef BUFFER_MGR_H
#define BUFFER_MGR_H

#include "MyDB_PageHandle.h"
#include "MyDB_Table.h"
#include <map>
#include <vector>
#include <memory>

using namespace std;

class MyDB_Page;
typedef shared_ptr<MyDB_Page> MyDB_PagePtr;

class MyDB_BufferManager {

public:

	// THESE METHODS MUST APPEAR AND THE PROTOTYPES CANNOT CHANGE!

	// gets the i^th page in the table whichTable... note that if the page
	// is currently being used (that is, the page is current buffered) a handle 
	// to that already-buffered page should be returned
	MyDB_PageHandle getPage (MyDB_TablePtr whichTable, long i);

	// gets a temporary page that will no longer exist (1) after the buffer manager
	// has been destroyed, or (2) there are no more references to it anywhere in the
	// program.  Typically such a temporary page will be used as buffer memory.
	// since it is just a temp page, it is not associated with any particular 
	// table
	MyDB_PageHandle getPage ();

	// gets the i^th page in the table whichTable... the only difference 
	// between this method and getPage (whicTable, i) is that the page will be 
	// pinned in RAM; it cannot be written out to the file
	MyDB_PageHandle getPinnedPage (MyDB_TablePtr whichTable, long i);

	// gets a temporary page, like getPage (), except that this one is pinned
	MyDB_PageHandle getPinnedPage ();

	// un-pins the specified page
	void unpin (MyDB_PageHandle unpinMe);

	// creates an LRU buffer manager... params are as follows:
	// 1) the size of each page is pageSize 
	// 2) the number of pages managed by the buffer manager is numPages;
	// 3) temporary pages are written to the file tempFile
	MyDB_BufferManager (size_t pageSize, size_t numPages, string tempFile);
	
	// when the buffer manager is destroyed, all of the dirty pages need to be
	// written back to disk, any necessary data needs to be written to the catalog,
	// and any temporary files need to be deleted
	~MyDB_BufferManager ();

	// FEEL FREE TO ADD ADDITIONAL PUBLIC METHODS 
	void EvictPage(MyDB_PagePtr page);
	void LoadPageFromDisk(MyDB_PagePtr page);
	void WritePageToDisk(MyDB_PagePtr page);
	void DeallocateBuffer(void* buffer);
	bool HasFreeBuffer();
	void* AllocateBuffer();
	MyDB_PagePtr FindLRUPage();
	long getNextTimeStamp(){ return TimeStamp++; }

private:

	// YOUR STUFF HERE
	vector<void*> buffer;
	vector<bool> bufferTaken;
	size_t pageSize;
	size_t numPages;
    string tempFile;
	map<pair<MyDB_TablePtr, long>, MyDB_PagePtr> pageTableMap;
	long nextTempPageID;
	long TimeStamp;
};

class MyDB_Page : public std::enable_shared_from_this<MyDB_Page> {
	public:
		MyDB_Page(MyDB_TablePtr table, long pageId, bool pinned, MyDB_BufferManager& bufMgr);
		~MyDB_Page();
		
		void* GetBytes();
		void WroteBytes();
		void AddRef();
		void RemoveRef();
		
		bool IsPinned() const { return pinned; }
		void SetPinned(bool p) { pinned = p; }
		bool IsDirty() const { return dirty; }
		void SetDirty(bool d) { dirty = d; }
		bool IsBuffered() const { return buffered; }
		void SetBuffered(bool b) { buffered = b; }
		
		long GetTimestamp() const { return timestamp; }
		void SetTimestamp(long t) { timestamp = t; }
		
		MyDB_TablePtr GetTable() const { return table; }
		long GetPageId() const { return pageId; }
		int GetRefCount() const { return refCount; }
		
		void SetBuffer(void* buf) { buffer = buf; }
		void* GetBuffer() const { return buffer; }

	private:
		MyDB_TablePtr table;
		long pageId;
		bool pinned;
		bool dirty;
		bool buffered;
		void* buffer;
		int refCount;
		long timestamp;
		MyDB_BufferManager& bufferManager;
};
#endif


