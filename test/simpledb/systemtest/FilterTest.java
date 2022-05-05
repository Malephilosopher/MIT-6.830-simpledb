package simpledb.systemtest;

import static org.junit.Assert.*;

import jav.simpledb.common.DbException;
import jav.simpledb.execution.Filter;
import jav.simpledb.execution.Predicate;
import jav.simpledb.execution.SeqScan;
import jav.simpledb.storage.HeapFile;
import jav.simpledb.transaction.TransactionAbortedException;
import jav.simpledb.transaction.TransactionId;

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
