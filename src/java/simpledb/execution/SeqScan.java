package java.simpledb.execution;

import java.simpledb.common.Database;
import java.simpledb.common.DbException;
import java.simpledb.storage.DbFile;
import java.simpledb.storage.DbFileIterator;
import java.simpledb.storage.Tuple;
import java.simpledb.storage.TupleDesc;
import java.simpledb.transaction.TransactionAbortedException;
import java.simpledb.transaction.TransactionId;
import java.simpledb.common.Type;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;

    private int tableId;

    private String tableAlias;

    private DbFileIterator iterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableId = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        DbFile table = Database.getCatalog().getDatabaseFile(tableId);
        iterator = table.iterator(tid);
        iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc tupleDesc =  Database.getCatalog().getTupleDesc(tableId);
        String prefix = "null";

        if(tableAlias != null){
            prefix = tableAlias;
        }
        int length = tupleDesc.numFields();
        Type[] typeAr = new Type[length];
        String[] fieldAr = new String[length];
        for(int i=0; i<length; i++){
            typeAr[i] = tupleDesc.getFieldType(i);
            String filedName = "null";
            if(tupleDesc.getFieldName(i) != null){
                filedName = tupleDesc.getFieldName(i);
            }
            fieldAr[i] = prefix + "." + filedName;
        }
        tupleDesc = new TupleDesc(typeAr,fieldAr);
        return tupleDesc;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(iterator != null){
            return iterator.hasNext();
        }
        throw new TransactionAbortedException();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if(iterator == null || !hasNext()){
            throw new NoSuchElementException("This is the last element");
        }
        Tuple next = iterator.next();
        if(next == null) throw new NoSuchElementException("This is the last element");
        return next;

    }

    public void close() {
         this.iterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        close();
        open();
    }
}
