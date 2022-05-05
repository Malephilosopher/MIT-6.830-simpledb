package jav.simpledb.storage;

import jav.simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * The list for items
     */
    private List<TDItem> items;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }


    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        this.items = new ArrayList<>();
        int len = typeAr.length;
        for (int i = 0; i < len; i++) {
            Type type = typeAr[i];
            String fa = fieldAr[i] == null ? "" : fieldAr[i];
            items.add(new TDItem(type, fa));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this.items = new ArrayList<>();
        int len = typeAr.length;
        for (int i = 0; i < len; i++) {
            Type type = typeAr[i];
            items.add(new TDItem(type, ""));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        int size = items.size();
        if(i < 0 || i >= size || items.get(i) == null){
            throw new NoSuchElementException("i is not a valid field reference");
        }
        return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        int size = items.size();
        if(i < 0 || i >= size || items.get(i) == null){
            throw new NoSuchElementException("i is not a valid field reference");
        }
        return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if(name == null)throw new NoSuchElementException("null is not a valid field name");
        for (int i = 0; i < items.size(); i++) {
            String s = items.get(i).fieldName;
            if(name.equals(s)){
                return i;
            }
        }

        throw new NoSuchElementException("no field with a matching name is found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int res = 0;
        for (int i = 0; i < items.size(); i++) {
            res += items.get(i).fieldType.getLen();
        }
        return res;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int s1 = td1.numFields();
        int s2 = td2.numFields();
        Type[] typeList = new Type[s1 + s2];
        for (int i = 0; i < s1; i++) {
            typeList[i] = td1.getFieldType(i);
        }
        for (int i = s1; i < s1 + s2; i++) {
            typeList[i] = td2.getFieldType(i - s1);
        }
        String[] nameList = new String[s1 + s2];
        for (int i = 0; i < s1; i++) {
            nameList[i] = td1.getFieldName(i);
        }
        for (int i = s1; i < s1 + s2; i++) {
            nameList[i] = td2.getFieldName(i - s1);
        }
        return new TupleDesc(typeList, nameList);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        //check the type of class
        if(o == null || o.getClass() != TupleDesc.class){
            return false;
        }
        TupleDesc that = (TupleDesc) o;
        int len1 = this.numFields();
        int len2 = that.numFields();
        if(len1 != len2){
            return false;
        }
        for (int i = 0; i < len1; i++) {
            if(!this.items.get(i).fieldType.equals(that.items.get(i).fieldType)){
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        int hashcode = 10;
        for (int i = 0; i < items.size(); i++) {
            Type type = items.get(i).fieldType;
            String name = items.get(i).fieldName;
            int hashType = type.hashCode();
            int hashName = name.hashCode();
            hashcode = 37 * hashcode + hashType;
            hashcode = 37 * hashcode + hashName;
        }
        return hashcode;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < items.size(); i++) {
            sb.append(items.get(i).fieldType).append(items.get(i).fieldName).append(", ");
        }
        return sb.toString();
    }
}
