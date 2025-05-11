/************************************************************************************
 * @file LinHashMap.java
 *
 * @author  John Miller
 *
 * compile javac --enable-preview --release 21 LinHashMap.java
 * run     java --enable-preview LinHashMap
 */

 import java.io.*;
import static java.lang.System.out;
import java.lang.reflect.Array;
 import java.util.*;
 

 /************************************************************************************
  * The `LinHashMap` class provides hash maps that use the Linear Hashing algorithm.
  * A hash table is created that is an expandable array-list of buckets.
  */
 
 public class LinHashMap<K, V> extends AbstractMap<K, V> implements Serializable, Cloneable, Map<K, V> {
     private static final boolean DEBUG = true;
     private static final int SLOTS = 4;
     private static final double THRESHOLD = 1.1;
 
     private final Class<K> classK;
     private final Class<V> classV;
     private final boolean unique;
 
     private final List<Bucket> hTable;
     private int mod1;
     private int mod2;
     private int isplit = 0;
     private int kCount = 0;
     private final Map<String, LinHashMap<K, List<V>>> indexMap = new HashMap<>();
 
     /************************************************************************************
      * Bucket inner class for storing key-value pairs.
      */
     private class Bucket implements Serializable {
         int keys;
         K[] key;
         V[] value;
         Bucket next;
 
         @SuppressWarnings("unchecked")
         Bucket() {
             keys = 0;
             key = (K[]) Array.newInstance(classK, SLOTS);
             value = (V[]) Array.newInstance(classV, SLOTS);
             next = null;
         }
 
         V find(K k) {
             for (int j = 0; j < keys; j++)
                 if (key[j].equals(k)) return value[j];
             return null;
         }
 
         void add(K k, V v) {
             key[keys] = k;
             value[keys] = v;
             keys++;
         }
         void print ()
         {
             out.print ("[ " );
             for (var j = 0; j < keys; j++) out.print (key[j] + " . ");
             out.println ("]" );
         } // print
     }
 
     /************************************************************************************
      * Constructor for LinHashMap with a unique flag.
      */
     public LinHashMap(Class<K> _classK, Class<V> _classV, boolean unique) {
         this.classK = _classK;
         this.classV = _classV;
         this.unique = unique;
         this.mod1 = 4;
         this.mod2 = 2 * mod1;
         this.hTable = new ArrayList<>();
         for (int i = 0; i < mod1; i++) hTable.add(new Bucket());
     }
 
     public LinHashMap(Class<K> _classK, Class<V> _classV) {
         this(_classK, _classV, false);
     }
 
     /************************************************************************************
      * Indexing Methods
      */
     public void create_index(String attributeName) {
         if (!indexMap.containsKey(attributeName)) {
             indexMap.put(attributeName, new LinHashMap<>(classK, (Class<List<V>>) (Class<?>) List.class, false));
         }
     }
 
     public void create_unique_index(String attributeName) {
         if (!indexMap.containsKey(attributeName)) {
             indexMap.put(attributeName, new LinHashMap<>(classK, (Class<List<V>>) (Class<?>) List.class, true));
         }
     }
 
     public void drop_index(String attributeName) {
         indexMap.remove(attributeName);
     }
 
     /************************************************************************************
      * Returns the set of key-value entries.
      */
     @Override
     public Set<Map.Entry<K, V>> entrySet() {
         Set<Map.Entry<K, V>> entrySet = new HashSet<>();
         for (Bucket bucket : hTable) {
             for (Bucket b = bucket; b != null; b = b.next) {
                 for (int i = 0; i < b.keys; i++) {
                     entrySet.add(new AbstractMap.SimpleEntry<>(b.key[i], b.value[i]));
                 }
             }
         }
         return entrySet;
     }
 
     private int h(Object key) { return Math.abs(key.hashCode()) % mod1; }
     private int h2(Object key) { return Math.abs(key.hashCode()) % mod2; }
 
     public V get(Object key) {
         int i = h(key);
         return find((K) key, hTable.get(i));
     }
 
     private V find(K key, Bucket bh) {
         for (Bucket b = bh; b != null; b = b.next) {
             V result = b.find(key);
             if (result != null) return result;
         }
         return null;
     }
 
     public V put(K key, V value) {
         int i = h(key);
         Bucket bh = hTable.get(i);
         V oldV = find(key, bh);
 
         if (unique && oldV != null) {
             for (Bucket b = bh; b != null; b = b.next) {
                 for (int j = 0; j < b.keys; j++) {
                     if (b.key[j].equals(key)) {
                         V oldValue = b.value[j];
                         b.value[j] = value;
                         return oldValue;
                     }
                 }
             }
             return null;
         } else {
             kCount++;
         }
 
         if (loadFactor() > THRESHOLD) split();
 
         for (Bucket b = bh; ; b = b.next) {
             if (b.keys < SLOTS) {
                 b.add(key, value);
                 updateSecondaryIndexes(key, value);
                 return oldV;
             }
             if (b.next == null) {
                 b.next = new Bucket();
                 b.next.add(key, value);
                 updateSecondaryIndexes(key, value);
                 return oldV;
             }
         }
     }
 
     /************************************************************************************
      * Splits a bucket and redistributes keys when the load factor is exceeded.
      */
     private void split() {
         int oldBucketIndex = isplit;
         Bucket oldChain = hTable.get(oldBucketIndex);
         int newBucketIndex = mod1 + isplit;
 
         while (hTable.size() <= newBucketIndex) {
             hTable.add(new Bucket());
         }
 
         List<Map.Entry<K, V>> entriesToMove = new ArrayList<>();
         Bucket current = oldChain;
         while (current != null) {
             int i = 0;
             while (i < current.keys) {
                 K key = current.key[i];
                 V value = current.value[i];
                 int hash2 = h2(key);
 
                 if (hash2 == newBucketIndex) {
                     entriesToMove.add(new AbstractMap.SimpleEntry<>(key, value));
                     System.arraycopy(current.key, i+1, current.key, i, current.keys - i - 1);
                     System.arraycopy(current.value, i+1, current.value, i, current.keys - i - 1);
                     current.keys--;
                 } else {
                     i++;
                 }
             }
             current = current.next;
         }
 
         Bucket newChain = hTable.get(newBucketIndex);
         for (Map.Entry<K, V> entry : entriesToMove) {
             K key = entry.getKey();
             V value = entry.getValue();
             Bucket b = newChain;
             while (true) {
                 if (b.keys < SLOTS) {
                     b.add(key, value);
                     break;
                 }
                 if (b.next == null) {
                     b.next = new Bucket();
                     b.next.add(key, value);
                     break;
                 }
                 b = b.next;
             }
         }
 
         isplit++;
         if (isplit >= mod1) {
             mod1 = mod2;
             mod2 *= 2;
             isplit = 0;
         }
     }
 
     private void updateSecondaryIndexes(K key, V value) {
         for (String attr : indexMap.keySet()) {
             LinHashMap<K, List<V>> index = indexMap.get(attr);
             List<V> values = index.get(key);
             if (values == null) {
                 values = new ArrayList<>();
                 index.put(key, values);
             }
             values.add(value);
         }
     }
 
     private double loadFactor() { return kCount / (double) (SLOTS * (mod1 + isplit)); }
 
    /********************************************************************************
      * Print the linear hash table.
      */
      public void printT ()
      {
          out.println ("LinHashMap");
          out.println ("-------------------------------------------");
  
          for (var i = 0; i < hTable.size (); i++) {
              out.print ("Bucket [ " + i + " ] = ");
              var j = 0;
              for (var b = hTable.get (i); b != null; b = b.next) {
                  if (j > 0) out.print (" \t\t --> ");
                  b.print ();
                  j++;
              } // for
          } // for
  
          out.println ("-------------------------------------------");
      } // printT
    //testing methods in main
     public static void main(String[] args) {
        /** Test Case One, please recomment if you want to use this section and bracket the others
        LinHashMap<Integer, Integer> map = new LinHashMap<>(Integer.class, Integer.class, true);
         map.put(1, 100);
         map.put(2, 200);
         map.put(3, 300);
         map.create_index("TestIndex");
         map.create_unique_index("UniqueTest");
         map.drop_index("TestIndex");

         */
        /** Test Case Two, recmomment if you want to use this section */
        LinHashMap<Integer, String> map = new LinHashMap<>(Integer.class, String.class, true);
        
        // Insert test data
        System.out.println("Inserting values into LinHashMap...");
         map.put(1, "Alice");
         map.put(2, "Bob");
         map.put(3, "Charlie");
         map.put(4, "David");
         map.put(5, "Eve");
         map.put(6, "Frank");
         map.put(7, "Grace");
         map.put(8, "Hank");
         map.put(9, "Ivy");
         map.put(10, "Jack");
    
         // Print initial contents
         System.out.println("Initial LinHashMap contents:");
         for (Map.Entry<Integer, String> entry : map.entrySet()) {
             System.out.println(entry.getKey() + " -> " + entry.getValue());
         }

        System.out.println("Testing Indexed Select...");
         String result = map.get(5);  // Should return "Eve"
         System.out.println("Result for key 5: " + result);
    
         System.out.println("Testing Indexing Methods...");
    
         // Create indices
         map.create_index("TestIndex");
         map.create_unique_index("UniqueTest");
    
         // Drop an index
         map.drop_index("TestIndex");
    
         System.out.println("Indexing methods executed successfully.");
         /** Orgianl test case: please leave this in main on future commits */
         var totalKeys = 40;
         var RANDOMLY  = false;
 
         LinHashMap <Integer, Integer> ht = new LinHashMap <> (Integer.class, Integer.class);
         if (args.length == 1) totalKeys = Integer.valueOf (args [0]);
 
         if (RANDOMLY) {
             var rng = new Random ();
             for (var i = 1; i <= totalKeys; i += 2) ht.put (rng.nextInt (2 * totalKeys), i * i);
         } else {
             for (var i = 1; i <= totalKeys; i += 2) ht.put (i, i * i);
         } // if
 
         ht.printT ();
         for (var i = 0; i <= totalKeys; i++) {
             out.println ("key = " + i + ", value = " + ht.get (i));
         } // for
         out.println ("-------------------------------------------");
         out.println ("Average number of buckets accessed = " + (ht.kCount / (double) totalKeys));
        
        //accessing ht
        ht.hTable.get(1).next.print();
        out.println(ht.indexMap.isEmpty());
        ht.create_index("Movies");
        out.println(ht.indexMap.isEmpty());
     }
    

 } //LinHashMap
 






