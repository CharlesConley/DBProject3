class BufMgr {
    private :
    FrameId clockHand; // clock hand for clock algorithm
    BufHashTbl ∗hashTable ; // hash table mapping ( File , page) to frame number
    BufDesc ∗bufDescTable ; // BufDesc objects , one per frame
    std : : uint32_t numBufs ; // Number of frames in the buffer pool
    BufStats bufStats; // Statistics about buffer pool usage
    
    // allocate a free frame using the clock algorithm
    void allocBuf(FrameId & frame);
    void advanceClock () ; //Advance clock to next frame in the buffer pool
    public :
    Page ∗bufPool ; // actual buffer pool
    BufMgr( std : : uint32_t bufs ) ; // Constructor
    ~BufMgr ( ) ; // Destructor
    
    void readPage(File∗ file , const PageId PageNo, Page∗& page);
    void unPinPage(File∗ file , const PageId PageNo, const bool dirty);
    void allocPage(File∗ file , PageId& PageNo, Page∗& page);
    void disposePage( File∗ file , const PageId pageNo) ;
    void flushFile(const File∗ file);
};
