package simpledb.systemtest;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import java.simpledb.common.DbException;
import java.simpledb.execution.Delete;
import java.simpledb.execution.Filter;
import java.simpledb.execution.Predicate;
import java.simpledb.execution.SeqScan;
import java.simpledb.storage.HeapFile;
import java.simpledb.storage.IntField;
import java.simpledb.storage.Tuple;
import java.simpledb.transaction.TransactionAbortedException;
import java.simpledb.transaction.TransactionId;

public class DeleteTest extends FilterBase {
    List<List<Integer>> expectedTuples = null;

    @Override
    protected int applyPredicate(HeapFile table, TransactionId tid, Predicate predicate)
            throws DbException, TransactionAbortedException {
        SeqScan ss = new SeqScan(tid, table.getId(), "");
        Filter filter = new Filter(predicate, ss);
        Delete deleteOperator = new Delete(tid, filter);
//        Query q = new Query(deleteOperator, tid);

//        q.start();
        deleteOperator.open();
        boolean hasResult = false;
        int result = -1;
        while (deleteOperator.hasNext()) {
            Tuple t = deleteOperator.next();
            assertFalse(hasResult);
            hasResult = true;
            assertEquals(SystemTestUtil.SINGLE_INT_DESCRIPTOR, t.getTupleDesc());
            result = ((IntField) t.getField(0)).getValue();
        }
        assertTrue(hasResult);

        deleteOperator.close();

        // As part of the same transaction, scan the table
        if (result == 0) {
            // Deleted zero tuples: all tuples still in table
            expectedTuples = createdTuples;
        } else {
            assert result == createdTuples.size();
            expectedTuples = new ArrayList<>();
        }
        SystemTestUtil.matchTuples(table, tid, expectedTuples);
        return result;
    }

    @Override
    protected void validateAfter(HeapFile table)
            throws DbException, TransactionAbortedException, IOException {
        // As part of a different transaction, scan the table
        SystemTestUtil.matchTuples(table, expectedTuples);
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(DeleteTest.class);
    }
}
