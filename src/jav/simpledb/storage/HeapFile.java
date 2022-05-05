package jav.simpledb.storage;

import jav.simpledb.common.Database;
import jav.simpledb.transaction.TransactionAbortedException;
import jav.simpledb.transaction.TransactionId;
import jav.simpledb.common.DbException;
import jav.simpledb.common.Permissions;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private int tableId;

    private int tuplesPerPage;

    private int headerBytes;

    private File file;

    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td){
        // some code goes here
        tableId = f.getAbsoluteFile().hashCode();
        int tupleSize = td.getSize();
        int pageSize = BufferPool.getPageSize();
        tuplesPerPage = (int) Math.floor((pageSize * 8) / (tupleSize * 8 + 1));
        this.headerBytes = (int) Math.ceil(tuplesPerPage / 8);
        this.file = f;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an tableId uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an tableId uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageNo = pid.getPageNumber();
        HeapPage heapPage = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "r");
            //skip the pages before
            raf.seek((long) pageNo * tuplesPerPage);
            byte[] data = new byte[tuplesPerPage + headerBytes];
            for (int i = 0; i < headerBytes; i++) {
                //initialize the headers by 1
                data[i] = (byte) 255;
            }
            if(raf.read(data, headerBytes, tuplesPerPage) == -1){
                return null;
            }
            heapPage = new HeapPage(new HeapPageId(pid.getTableId(), pageNo), data);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("file not found");
        }catch (IOException e){
            throw new IllegalArgumentException("Page with pageNo: " + pageNo + " not found");
        }finally {
            try {
                raf.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.floor(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator{

        private final TransactionId tid;
        private Iterator<Tuple> tupsIterator;
        private final int tableId;
        private final int numPages;
        private int pageNo;

        public HeapFileIterator(TransactionId transactionId) {
            this.tid = transactionId;
            tableId = getId();
            numPages = numPages();
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageNo = 0;
            tupsIterator = getTuplesIterator(pageNo);
        }

        /**
         * util method to get the tupleiterator of a specific page. At the beginning,
         * obtain the iterator of the first page
         * @param pageNumber
         * @return
         * @throws DbException
         * @throws TransactionAbortedException
         */
        private Iterator<Tuple> getTuplesIterator(int pageNumber) throws DbException, TransactionAbortedException {
            if(pageNumber >= 0 && pageNumber < numPages){
                HeapPageId heapPageId = new HeapPageId(tableId,pageNumber);
                HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
                return heapPage.iterator();
            } else {
                throw new DbException(String.format("heapFile %d does not contain page %d!",tableId, pageNumber));
            }

        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(tupsIterator == null){//hasn't opened
                return false;
            }
            if(!tupsIterator.hasNext()){//if the current page has been fully read
                if(pageNo < (numPages-1)){
                    pageNo++;//switch to next page and get the tupleIterator
                    tupsIterator = getTuplesIterator(pageNo);
                    return tupsIterator.hasNext();
                }else {
                    return false;
                }
            } else {
                return true;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(tupsIterator==null || !tupsIterator.hasNext()){
                throw new NoSuchElementException("This is the last element");
            }
            return tupsIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            tupsIterator = null;
        }
    }

}

