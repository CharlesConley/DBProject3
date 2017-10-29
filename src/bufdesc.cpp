class BufDesc {
    friend class BufMgr;
    private :
    File∗ file ; // pointer to file object
    PageId pageNo ; // page within f i l e
    FrameId frameNo ; // buffer pool frame number
    int pinCnt; // number of times this page has been pinned
    bool dirty; // true if dirty ; false otherwise
    bool valid; // true if page is valid
    bool refbit; // true if this buffer frame been referenced recently
    
    void Clear (); // Initialize buffer frame
    void Set(File∗ filePtr , PageId pageNum); //set BufDesc member variable values
    void Print () //Print values of member variables
    BufDesc ( ) ; //Constructor
};





