/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

 /**
  * @author Databases Project 3 (Buffer Manager): Charles Conley, Josh Cordell, Bryce Greiber
  */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

/**
 *  Destructor for BufMgr. For all dirty pages, flush the file associated. This will write the most up to date content to the disk.
 *  Deallocate all data structures from the constructor
 */
BufMgr::~BufMgr() {
    
    ///flush files
    for(uint32_t j = 0; j > numBufs; j++){
        if(bufDescTable[j].dirty){
            flushFile(bufDescTable[j].file);
        }
    }
    
    
    ///Now
    delete hashTable;
    delete[] bufPool;
    delete[] bufDescTable;
    
    
    
}

/**
*  Move the "hand" pointer along the buffer clock. use the remainder to stay in bounds.
* Input: Address to frame (for reference return)
* Output: frame as a referenced value.
*/
void BufMgr::advanceClock()
{
    clockHand = (clockHand + 1) % numBufs;
}

///Implement the clock algorithm
///add a pinned count, if pinned count = numBufs, then throw exception
///run through each buf and check for validity following diagram.
void BufMgr::allocBuf(FrameId & frame) 
{
    ///track if a frame has been found
    bool frameFound = false;
    
    std::uint32_t pinnedCount = 0;
    
    ///start loop, until frame is found. Only time it will leave loop is if frame is found
    ///or bufferexceededexception occurs
    while(!frameFound && (pinnedCount < numBufs)){
        
        advanceClock();
        
        ///found a buffer frame that can be used, exit loop
        if(!bufDescTable[clockHand].valid){
        
            frameFound = true;
            break;
        
        } ///check the refbit, if it is false then frame is found and the entry in the hashtable needs to be removed
        else if(bufDescTable[clockHand].refbit){
            bufDescTable[clockHand].refbit = false;
            continue;
        }else if(bufDescTable[clockHand].pinCnt > 0) {
            pinnedCount++;
            continue;
        }else{
            
            if(bufDescTable[clockHand].dirty){
                bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
            }
            frameFound = true;
            ///remove the hashtable entry
            hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
        }
    
    }
    ///All pages are pinned, otherwise found free frame
    if(pinnedCount == numBufs){
        throw BufferExceededException();
    } else {
        frame = clockHand;
        
        bufDescTable[clockHand].Clear();
    }
    
}

/**
*  Check to see if the page is in the buffer, if so then return the pointer to the page
*  If the page is not located in the buffer then add it to the buffer and return the page pointer.
 * Input: file pointer, pageNo, address of page(for reference return)
 * Outpu: Returns the address of a page for reading
 */
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
    FrameId frameNo;
    /// check whether page is already in buffer pool with lookup(), throws HashNotFoundExc.
    try {
        hashTable->lookup(file, pageNo, frameNo); /// no exception thrown, in hash table

        /// set appropriate refbit
        bufDescTable[frameNo].refbit = 1;

        /// increment pin count
        bufDescTable[frameNo].pinCnt++;
        /// return pointer to frame containing the page via page parameter
        page = &bufPool[frameNo];
    }
    catch (const HashNotFoundException& e) {
        /// not in buffer pool, need to add to buffer

        /// allocate buffer frame
        allocBuf(frameNo);
        /// add to bufPool
        bufPool[frameNo] = file->readPage(pageNo);
        /// set the description table
        bufDescTable[frameNo].Set(file, pageNo);
        /// insert page into hash table
        hashTable->insert(file, pageNo, frameNo);
        /// return the page pointer
        page = &bufPool[frameNo];
    }

}
    
/**
 *  updates the descTable to correlate with a page that is no longer pinned in the buffer
 *  Throws exception if the page is not pinned. Does not throw anything if the page is not in the buffer
 * Input: file pointer, pageNo, boolean dirty
 * Outpu: N/A
 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frameNo = 0;		
	///check hashtable for page
	try {
		hashTable->lookup(file, pageNo, frameNo);
		///check if pin cnt is already set to 0. throw appropriate error
		if (bufDescTable[frameNo].pinCnt == 0){
		throw PageNotPinnedException(file->filename(), pageNo, frameNo);	
		}
		///set dirty bit if input bit is true
        if(dirty){
            bufDescTable[frameNo].dirty = dirty;
        }
            
		///decrement pin count
		bufDescTable[frameNo].pinCnt--;	
		
	}
	catch(const HashNotFoundException& e){
		
	}

	
    
}

/**
 *  create a page within a file when one does not already exist
 *  Add to buffer since it is newly created and this is a form of access
 * Input: file pointer, pageNo, address of page(for reference return)
 * Output: returns a page address that is now allocated
 */
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	FrameId frameNo;		
	///allocate page and get buffer frame pool
	Page filePage = file->allocatePage();
	allocBuf(frameNo);
	pageNo = filePage.page_number();
	
	///insert into hashtable then set frame
	hashTable->insert(file, pageNo, frameNo);
	bufDescTable[frameNo].Set(file, pageNo);
	bufPool[frameNo] = filePage;
	///passs correct pointer
    page = &bufPool[frameNo];
	


}

/**
 *  Clears files from the buffer table. The important part of this function is to write to disk the changed pages.
 *  If the page is not valid or if the page is pinned then this will cause exceptions to be thrown.
 * Input: file pointer
 * Outpu: N/A
 */
void BufMgr::flushFile(const File* file) 
{
    for(uint32_t i = 0; i < numBufs; i++){
        //check to see if the entry is from this file
        if(file == bufDescTable[i].file){
          
            //before proceeding, check valid bit and pinned
            if(!bufDescTable[i].valid){
        	  throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
            }else if(bufDescTable[i].pinCnt > 0) {
           	  throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, i);
		 }
            ///Check for dirty page which will need to be written to disk
            if (bufDescTable[i].dirty){
                
                bufDescTable[i].file->writePage(bufPool[i]);
                bufDescTable[i].dirty = false;
            }
            ///remove the page and clear the buffer
            hashTable->remove(file, bufDescTable[i].pageNo);
            
            bufDescTable[i].Clear();
            
        }
    }
}

/**
 * Dispose Page deletes the page from the hashtable if it is present but also deletes the page in the file.
 * If it is not in hashtable then ignores the table and only removes the page from the file.
 * Input: file pointer, pageNo
 * Outpu: N/A
 */
void BufMgr::disposePage(File* file, const PageId PageNo)
{
	FrameId frameNo = 0;
    /// if allocated in buffer pool, free it
    try {
        hashTable->lookup(file, PageNo, frameNo);
        /// remove page from hash table
        bufDescTable[frameNo].Clear();
        
        hashTable->remove(file, PageNo);
    }
    catch (const HashNotFoundException& e) {
        /// not in hash table, shouldn't need to do anything
    }

    file->deletePage(PageNo);
}


void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
