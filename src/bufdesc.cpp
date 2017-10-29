class BufDesc {
    friend class BufMgr;
    private :
    File∗ file ; // pointer to file object PageId pageNo ; // page within f i l e
    FrameId frameNo ; // buffer pool frame number
    int pinCnt; bool dirty ; bool valid; bool refbit;
    void Clear () ;
    void Set(File∗ filePtr , PageId pageNum); //set BufDesc member variable
    values
    void Print () //Print values of member variables BufDesc ( ) ; //Constructor
};
