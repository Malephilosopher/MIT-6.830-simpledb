package simpledb.systemtest;

import static org.junit.Assert.*;

import java.simpledb.common.DbException;
import java.simpledb.execution.Filter;
import java.simpledb.execution.Predicate;
import java.simpledb.execution.SeqScan;
import java.simpledb.storage.HeapFile;
import java.simpledb.transaction.TransactionAbortedException;
import java.simpledb.transaction.TransactionId;

public class FilterTest extends FilterBase {
    @Override
    protected int applyPredicate(HeapFile table, TransactionId tid, Predicate predicate)
            throws DbException, TransactionAbortedException {
        SeqScan ss = new SeqScan(tid, table.getId(), "");
        Filter filter = new Filter(predicate, ss);
        filter.open();

        int resultCount = 0;
        while (filter.hasNext()) {
            assertNotNull(filter.next());
            resultCount += 1;
        }

        filter.close();
        return resultCount;
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(FilterTest.class);
    }
}
