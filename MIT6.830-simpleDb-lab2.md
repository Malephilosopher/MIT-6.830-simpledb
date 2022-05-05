# MIT6.830-simpleDb



## Lab2:

### Exercise1. Filter and join

This exercise require us to implement two operations of the relational algebra so that we can perform more complex queries. 

**Filter**: Filter the tuples according to the `Predicate` that is specified in the constructor.



**Join**: Join two tuples according to `JoinPredicate` specified in the constructor. A simple nested loops join is enough, but we may explore more complex ones.

Files to be implemented:

**Predicate**:

A predicate has three attributes: field, operation, operand. Take "id > 1" as an example. Here "id" is the field. Here we store the index of the field in the tuple's fields as the field. ">" is the operation, which means greater than. "1" is the operand, which is the field value to be compared to. In simpleDb, we store the value of a field as a Field(Which I find a bit comfusing). There are IntField and StringField. We just need to add them as attributes of the Predicate class and finish the getters and setters.



**JoinPredicate**:

Almost the same as Predicate. Just replace the Field2 for the operand. 



**Filter**:

The Filter class realizes the Operator interface, so remember to call the super's open and close method to set the open attributes. Other than that, nothing difficult about filter. The system design is really low-coupled, all we have to pay attention to is 



**Join**:

The Join class also realizes the Operator interface. However, the tricky part is how to realize nested loop join in the fetchNext method. You should memorize the result the last fetchNext get. In order to do this, I referenced the methods online and came up with the idea of caching the child1's tuple as the attribute when a match is found. The next time the fetchNext method is called, the search will start from this tuple. If no tuple from child2 matches the current tuple from child1, we will rewind child2 and switch to the next tuple1 from child1.

```java
protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        while (t1!=null||child1.hasNext()){
            if(t1==null){ // init t1 if null
                if(child1.hasNext()){
                    t1 = child1.next();
                }else{
                    return null;
                }
            }
            if(!child2.hasNext()){ // t2到头了，nextLoop
                if(child1.hasNext()){
                    child2.rewind();
                    t1 = child1.next();
                }else{
                    return null;
                }
            }
            while (child2.hasNext()){
                Tuple t2 = child2.next();
                if(joinPredicate.filter(t1,t2)){
                    Tuple res = new Tuple(getTupleDesc());
                    for (int i = 0; i < t1.getTupleDesc().numFields(); i++) {
                        res.setField(i,t1.getField(i));
                    }
                    for (int i = 0; i < t2.getTupleDesc().numFields(); i++) {
                        res.setField(t1.getTupleDesc().numFields()+i,t2.getField(i));
                    }
                    return res;
                }
            }
        }
        return null;
    }

```



