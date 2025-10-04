#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

// Include return codes and methods for logging errors
#include "dberror.h"

// Include bool DT
#include "dt.h"

// Replacement Strategies
typedef enum ReplacementStrategy {
	RS_FIFO = 0,
	RS_LRU = 1,
	RS_CLOCK = 2,
	RS_LFU = 3,
	RS_LRU_K = 4
} ReplacementStrategy;

// Data Types and Structures
typedef int PageNumber;
#define NO_PAGE -1

typedef struct BM_BufferPool {
	char *pageFile;  	  // page file name
	int numPages;         // number of frames in the buffer pool
	ReplacementStrategy strategy; // page replacement strategy
	void *mgmtData; // use this one to store the bookkeeping info your buffer
	// manager needs for a buffer pool
} BM_BufferPool;

typedef struct BM_PageHandle {
	PageNumber pageNum;    // this page number on disk
	char *data;           // this page data buffer
} BM_PageHandle;

//-----------------convenience macros-------------------
#define MAKE_POOL()					\
		((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
		((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))

//-----------------Buffer Manager Interface Pool Handling-------------------
/** 
* @brief initializes a buffer pool, no need to creating a new page file.
* @param bm, input value, a buffer pool structure pointer
* @param pageFileName, input value, a string pointer
* @param numPages, input value, an integer
* @param strategy, input value, a replacement strategy
* @param stratData, input value, a void pointer
*/
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
				const int numPages, ReplacementStrategy strategy,
				void *stratData);

/** 
* @brief shuts down a buffer pool, writing back all dirty pages to page file. 
*        And releases any resources associated with the buffer pool.
* @param bm, input value, a buffer pool structure pointer
*/
RC shutdownBufferPool(BM_BufferPool *const bm);

/** 
* @brief writing back all dirty pages in a buffer pool to page file.
* @param bm, input value, a buffer pool structure pointer
*/
RC forceFlushPool(BM_BufferPool *const bm);

//-------------------Buffer Manager Interface Access Pages-------------------
/** 
* @brief marks a page as dirty in the buffer pool
* @param bm, input value, a buffer pool structure pointer
* @param page, input value, a page handle structure pointer
*/
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page);

/** 
* @brief release a frame in the buffer pool, if needed, write back to page file.
* @param bm, input value, a buffer pool structure pointer
* @param page, input value, a page handle structure pointer
*/
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page);

/** 
* @brief forcePage forces a page to disk
* @param bm, input value, a buffer pool structure pointer
* @param page, input value, a page handle structure pointer
*/
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page);

/** 
* @brief load a page to frame
* @param bm, input value, a buffer pool structure pointer
* @param page, input value, a page handle structure pointer
* @param pageNum, input value, a page number
*/
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum);

//----------------------Statistics Interface----------------------
/** 
* @brief getFrameContents gets the contents of a frame
* @param bm, input value, a buffer pool structure pointer
* @return a page number array
*/
PageNumber *getFrameContents (BM_BufferPool *const bm);

/** 
* @brief getDirtyFlags gets the dirty flags of a frame
* @param bm, input value, a buffer pool structure pointer
* @return a boolean array
*/
bool *getDirtyFlags (BM_BufferPool *const bm);

/** 
* @brief getFixCounts gets the fix counts of a frame
* @param bm, input value, a buffer pool structure pointer
* @return an integer array
*/
int *getFixCounts (BM_BufferPool *const bm);

/** 
* @brief getNumReadIO gets the number of read I/O operations
* @param bm, input value, a buffer pool structure pointer
* @return an integer
*/	
int getNumReadIO (BM_BufferPool *const bm);

/** 
* @brief getNumWriteIO gets the number of write I/O operations
* @param bm, input value, a buffer pool structure pointer
* @return an integer
*/
int getNumWriteIO (BM_BufferPool *const bm);

#endif
