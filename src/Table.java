/****************************************************************************************
 * @file  Table.java
 *
 * @author   John Miller
 *
 * compile javac *.java
 * run     java MovieDB    
 */

 import java.io.*;
 import java.util.*;
 import java.util.function.*;
 import java.util.stream.*;
 
 import static java.lang.Boolean.*;
 import static java.lang.System.arraycopy;
 import static java.lang.System.out;


 /****************************************************************************************
  * The Table class implements relational database tables (including attribute names, domains
  * and a list of tuples.  Five basic relational algebra operators are provided: project,
  * select, union, minus and join.  The insert data manipulation operator is also provided.
  * Missing are update and delete data manipulation operators.
  */
 public class Table
        implements Serializable
 {
     /** Relative path for storage directory
      */
     private static final String DIR = "store" + File.separator;

     /** Filename extension for database files
      */
     private static final String EXT = ".dbf";
 
     /** Counter for naming temporary tables.
      */
     private static int count = 0;
 
     /** Table name.
      */
     private final String name;
 
     /** Array of attribute names.
      */
     final String [] attribute;
 
     /** Array of attribute domains: a domain may be
      *  integer types: Long, Integer, Short, Byte
      *  real types: Double, Float
      *  string types: Character, String
      */
     private final Class [] domain;
 
     /** Collection of tuples (data storage).
      */
     final List <Comparable []> tuples;
 
     /** Primary key (the attributes forming). 
      */
     private final String [] key;
 
     /** Index into tuples (maps key to tuple).
      */
     private final Map <KeyType, Comparable[]> index;
 
     /** The supported map types.
      */
     enum MapType { NO_MAP, TREE_MAP, HASH_MAP, LINHASH_MAP, BPTREE_MAP }
 
     /** The map type to be used for indices.  Change as needed.
      */
     static final MapType mType = MapType.NO_MAP;
 
     /************************************************************************************
      * Make a map (index) given the MapType.
      */
     private static Map <KeyType, Comparable []> makeMap ()
     {
         return switch (mType) {
         case NO_MAP      -> null;
         case TREE_MAP    -> new TreeMap <> ();
         case HASH_MAP    -> new HashMap <> ();
         case LINHASH_MAP -> new LinHashMap <> (KeyType.class, Comparable [].class);
         //case BPTREE_MAP  -> new BpTreeMap <> (KeyType.class, Comparable [].class);
         default          -> null;
         }; // switch
     } // makeMap
 
     /************************************************************************************
      * Concatenate two arrays of type T to form a new wider array.
      *
      * @see http://stackoverflow.com/questions/80476/how-to-concatenate-two-arrays-in-java
      *
      * @param arr1  the first array
      * @param arr2  the second array
      * @return  a wider array containing all the values from arr1 and arr2
      */
     public static <T> T [] concat (T [] arr1, T [] arr2)
     {
         T [] result = Arrays.copyOf (arr1, arr1.length + arr2.length);
         arraycopy (arr2, 0, result, arr1.length, arr2.length);
         return result;
     } // concat
 
     //-----------------------------------------------------------------------------------
     // Constructors
     //-----------------------------------------------------------------------------------
 
     /************************************************************************************
      * Construct an empty table from the meta-data specifications.
      *
      * @param _name       the name of the relation
      * @param _attribute  the string containing attributes names
      * @param _domain     the string containing attribute domains (data types)
      * @param _key        the primary key
      */  
     public Table (String _name, String [] _attribute, Class [] _domain, String [] _key)
     {
         name      = _name;
         attribute = _attribute;
         domain    = _domain;
         key       = _key;
         tuples    = new ArrayList <> ();
         index     = makeMap ();
         out.println (Arrays.toString (domain));
     } // constructor
 
     /************************************************************************************
      * Construct a table from the meta-data specifications and data in _tuples list.
      *
      * @param _name       the name of the relation
      * @param _attribute  the string containing attributes names
      * @param _domain     the string containing attribute domains (data types)
      * @param _key        the primary key
      * @param _tuples     the list of tuples containing the data
      */  
     public Table (String _name, String [] _attribute, Class [] _domain, String [] _key,
                   List <Comparable []> _tuples)
     {
         name      = _name;
         attribute = _attribute;
         domain    = _domain;
         key       = _key;
         tuples    = _tuples;
         index     = makeMap ();
     } // constructor
 
     /************************************************************************************
      * Construct an empty table from the raw string specifications.
      *
      * @param _name       the name of the relation
      * @param attributes  the string containing attributes names
      * @param domains     the string containing attribute domains (data types)
      * @param _key        the primary key
      */
     public Table (String _name, String attributes, String domains, String _key)
     {
         this (_name, attributes.split (" "), findClass (domains.split (" ")), _key.split(" "));
 
         out.println ("DDL> create table " + name + " (" + attributes + ")");
     } // constructor
 
     //----------------------------------------------------------------------------------
     // Public Methods
     //----------------------------------------------------------------------------------
    
    //-----------------------------------------------------------------------------------
    // Index Methods
    //-----------------------------------------------------------------------------------
    
     /**
      * This method creates a unique index based on the column names inputted. Intended to run with primary key and is a helper method
      * within Table.java
      * @param columnName - The columns to index off of
      */
     private void create_unique_index(String[] columnName) {
        for (Comparable[] tuple: this.tuples) {
            Comparable[] uniqueKeys = new Comparable[columnName.length];
            for (int i = 0; i < columnName.length; i++) {
                uniqueKeys[i] = tuple[i];
            } //for
            KeyType uniqueKey = new KeyType(uniqueKeys);
            this.index.put(uniqueKey, tuple);
        } //for
     } //create_unique_index

     /**
     * Remove key, value pair from table's index.
     * @param key
     */
     public void drop_index (String columnName) {
        KeyType keyTemp = new KeyType(columnName);
        index.remove(keyTemp);
        //this.index.remove(columnName);
      }

     /************************************************************************************
      * Project the tuples onto a lower dimension by keeping only the given attributes.
      * Check whether the original key is included in the projection.
      *
      * #usage movie.project ("title year studioNo")
      *
      * @param attributes  the attributes to project onto
      * @return  a table of projected tuples
      */
     public Table project (String attributes)
     {
         out.println ("RA> " + name + ".project (" + attributes + ")");
         var attrs     = attributes.split (" ");
         var colDomain = extractDom (match (attrs), domain);
         var newKey    = (Arrays.asList (attrs).containsAll (Arrays.asList (key))) ? key : attrs; //  New Key:  X (attrs), unless original key is preserved
         
         Table newTable = null;
        
         if (this.mType == MapType.NO_MAP) {
            List <Comparable []> rows = new ArrayList <> ();
            for (Comparable [] tuple: tuples) {
                if (rows.contains(tuple)) {
                    //do not add duplicates
                } else {
                    rows.add((extract(tuple, attrs)));
                } //if else
             } //for each
             newTable = new Table (name + count++, attrs, colDomain, newKey, rows);
         } else {
            newTable = new Table (name + count++, attrs, colDomain, newKey); //create new table
            for (Comparable [] tuple: tuples) {
                KeyType keyTemp = new KeyType(extract(tuple, newKey));
                if (!newTable.index.containsKey(keyTemp)) { //if key already in table, do not add -- removes duplicates
                    Comparable [] valTemp = extract(tuple, attrs);
                    newTable.tuples.add(valTemp);
                    newTable.index.put(keyTemp, valTemp);
                }
            }
        
        }
         return newTable;

         
 
    
     } // project
 
     /************************************************************************************
      * Select the tuples satisfying the given predicate (Boolean function).
      *
      * #usage movie.select (t -> t[movie.col("year")].equals (1977))
      *
      * @param predicate  the check condition for tuples
      * @return  a table with tuples satisfying the predicate
      */
     public Table select (Predicate <Comparable []> predicate)
     {
         out.println ("RA> " + name + ".select (" + predicate + ")");
 
         return new Table (name + count++, attribute, domain, key,
                    tuples.stream ().filter (t -> predicate.test (t))
                                    .collect (Collectors.toList ()));
     } // select
 
     /************************************************************************************
      * Select the tuples satisfying the given simple condition on attributes/constants
      * compared using an <op> ==, !=, <, <=, >, >=.
      *
      * #usage movie.select ("year == 1977")
      *
      * @param condition  the check condition as a string for tuples
      * @return  a table with tuples satisfying the condition
      */
     public Table select (String condition)
     {
         out.println ("RA> " + name + ".select (" + condition + ")");
 
         List <Comparable []> rows = new ArrayList <> ();

         // Split the condition into tokens: <attribute> <operator> <value>
         var token = condition.split (" ");
         if (token.length != 3) {
             throw new IllegalArgumentException("Condition must be in the format: <attribute> <operator> <value>");
         }
         // Find the column index for the attribute specified in the condition
         var colNo = col (token [0]);
         if (colNo == -1) {
             throw new IllegalArgumentException("Attribute '" + token[0] + "' not found in the table schema.");
         }

         for (var t : tuples) {
             // Check if the tuple satisfies the condition
             if (satifies (t, colNo, token [1], token [2])) rows.add (t);
         } // for
 
         return new Table (name + count++, attribute, domain, key, rows);
     } // select
 
     /************************************************************************************
      * Does tuple t satify the condition t[colNo] op value where op is ==, !=, <, <=, >, >=?
      *
      * #usage satisfies (t, 1, "<", "1980")
      *
      * @param colNo  the attribute's column number
      * @param op     the comparison operator
      * @param value  the value to compare with (must be converted, String -> domain type)
      * @return  whether the condition is satisfied
      */
     private boolean satifies (Comparable [] t, int colNo, String op, String value)
     {
         var t_A = t[colNo];
         out.println ("satisfies: " + t_A + " " + op + " " + value);
         var valt = switch (domain [colNo].getSimpleName ()) {      // type converted
         case "Byte"      -> Byte.valueOf (value);
         case "Character" -> value.charAt (0);
         case "Double"    -> Double.valueOf (value);
         case "Float"     -> Float.valueOf (value);
         case "Integer"   -> Integer.valueOf (value);
         case "Long"      -> Long.valueOf (value);
         case "Short"     -> Short.valueOf (value);
         case "String"    -> value;
         default          -> value;
         }; // switch
         var comp = t_A.compareTo (valt);
 
         return switch (op) {
         case "==" -> comp == 0;
         case "!=" -> comp != 0;
         case "<"  -> comp <  0;
         case "<=" -> comp <= 0;
         case ">"  -> comp >  0;
         case ">=" -> comp >= 0;
         default   -> false;
         }; // switch
     } // satifies
 
     /************************************************************************************
      * Select the tuples satisfying the given key predicate (key = value).  Use an index
      * (Map) to retrieve the tuple with the given key value.  INDEXED SELECT algorithm.
      *
      * @param keyVal  the given key value
      * @return  a table with the tuple satisfying the key predicate
      */
     public Table select (KeyType keyVal)
     {
         out.println ("RA> " + name + ".select (" + keyVal + ")");
        
         List <Comparable []> rows = new ArrayList <> ();
 
     // Create a LinHashMap for indexing
     Map<KeyType, Comparable[]> indexing;
     switch (this.mType) {
        case LINHASH_MAP:
            indexing = new LinHashMap<>(KeyType.class, Comparable[].class);
            break;
        case HASH_MAP:
            indexing = new HashMap<KeyType, Comparable[]>();
            break;
        case TREE_MAP:
            indexing = new TreeMap<KeyType, Comparable[]>();
            break;
        default:
            indexing = new LinHashMap<>(KeyType.class, Comparable[].class);
            break;
     }
     
    // Assuming the first attribute is the primary key
    // indexing.create_index(this.attribute[0]);

    // Populate the index with current table tuples
    for (Comparable[] tuple : this.tuples) {
        KeyType tupleKey = new KeyType(tuple[0]);  // Assuming the first attribute is the key
        indexing.put(tupleKey, tuple);
    }

    // Retrieve the tuple if it exists
    if (indexing.containsKey(keyVal)) {
        rows.add(indexing.get(keyVal));
        out.println(keyVal);
    }
   // indexing.drop_index(this.attribute[0]);
         return new Table(name + count++, attribute, domain, key, rows);
     } // select
 
     /************************************************************************************
      * Union this table and table2.  Check that the two tables are compatible.
      *
      * #usage movie.union (show)
      *
      * @param table2  the rhs table in the union operation
      * @return  a table representing the union
      */
     public Table union (Table table2)
     {
         out.println ("RA> " + name + ".union (" + table2.name + ")");
         if (! compatible (table2)) return null;
 
         List <Comparable []> rows = new ArrayList <> ();
        
         LinHashMap <KeyType, Comparable[]> indexing = new LinHashMap<>(KeyType.class, Comparable[].class);
      
        for (KeyType keyTOne: this.index.keySet()) {
            if (keyTOne != null) {
                indexing.put(keyTOne, this.index.get(keyTOne));
            } //if  
        } //for each
        for (KeyType keyTTwo: table2.index.keySet()) {
            if (keyTTwo != null) { 
                if (indexing.get(keyTTwo) == null) {
                    indexing.put(keyTTwo, table2.index.get(keyTTwo));
                } //if
            } //if
        } //for each
        for (KeyType keyNew: indexing.keySet()) {
            if (keyNew != null) {
                rows.add(indexing.get(keyNew));
            } //if
        } //for
        Table unionTable = new Table (name + count++, attribute, domain, key, rows);
        unionTable.create_unique_index(this.key); 
        return unionTable;
     } // union
 
     /************************************************************************************
      * Take the difference of this table and table2.  Check that the two tables are
      * compatible.
      *
      * #usage movie.minus (show)
      *
      * @param table2  The rhs table in the minus operation
      * @return  a table representing the difference
      */
     public Table minus (Table table2)
     {
        out.println ("RA> " + name + ".minus (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <> ();

        // USES OPTIMIZED LINHASH_MAP FOR MINUS IF mTYPE IS SPECIFIED TO USE LINHASHMAP
        // This will run in linear time since there is no need for nested for loops

        // uses old minus method otherwise (if linear hashmap mode is not turned on)
        if (mType == MapType.LINHASH_MAP) {
            System.out.println("Using optimized Lin_hashmap method!");
            LinHashMap <KeyType, Comparable[]> indexing = new LinHashMap<>(KeyType.class, Comparable[].class);
            // using primary key as first key
            indexing.create_index(this.attribute[0]);
            for (KeyType keyTOne: this.index.keySet()) {
                if (keyTOne != null) {
                    indexing.put(keyTOne, this.index.get(keyTOne));
                } //if 
            }

            for (Comparable[] tupleT: table2.tuples) {
                if (!indexing.containsKey(tupleT[0])) {
                    rows.add(tupleT);
                }
            } //for
        // old method that is not used when linear hashmap option is turned off
        } else {
            for (Comparable[] tupleT: tuples) {
                boolean equal = false; 
                for (Comparable[] tupleU: table2.tuples) {
                    if (tupleT[0].equals(tupleU[0])) {
                        equal = true; 
                    } //if
                } //for
                if (!equal) {
                    rows.add(tupleT);
                } //if
            } //for
        }
 
        return new Table (name + count++, attribute, domain, key, rows);
     } // minus
 
     /************************************************************************************
      * Join this table and table2 by performing an "equi-join".  Tuples from both tables
      * are compared requiring attributes1 to equal attributes2.  Disambiguate attribute
      * names by appending "2" to the end of any duplicate attribute name.  Implement using
      * a NESTED LOOP JOIN ALGORITHM.
      *
      * #usage movie.join ("studioName", "name", studio)
      *
      * @param attributes1  the attributes of this table to be compared (Foreign Key)
      * @param attributes2  the attributes of table2 to be compared (Primary Key)
      * @param table2       the rhs table in the join operation
      * @return  a table with tuples satisfying the equality predicate
      */
     public Table join (String attributes1, String attributes2, Table table2)
     {
         out.println ("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", " + table2.name + ")");
 
         var t_attrs = attributes1.split (" ");
         var u_attrs = attributes2.split (" ");

         var t_attrsIndexes = match(t_attrs);
         var u_attrsIndexes = table2.match(u_attrs);

         int numAttributes = t_attrs.length;

         for (int i = 0; i < t_attrs.length; i++) {
            for (int j = 0; j < u_attrs.length; j++) {
                if (attribute[i].equals(table2.attribute[j])) {
                    table2.attribute[j] = table2.attribute[j] + "2";
                }
            }
         }

         var rows    = new ArrayList <Comparable []> ();

         for (Comparable[] tupleT : tuples) {
            for (Comparable[] tupleU : table2.tuples) {
                boolean valid = true;

                for (int i = 0; i < numAttributes; i++) {
                    if (tupleT[t_attrsIndexes[i]].compareTo(tupleU[u_attrsIndexes[i]]) != 0) {
                        valid = false;
                    }
                }

                if (valid) {
                    Comparable[] row = new Comparable[tupleT.length + tupleU.length];
                    for (int i = 0; i < tupleT.length; i++) {
                        row[i] = tupleT[i];
                    }

                    for (int i = 0; i < tupleU.length; i++) {
                        row[tupleT.length + i] = tupleU[i];
                    }

                    rows.add(row);
                }
            }
         }
 
         return new Table (name + count++, concat (attribute, table2.attribute),
                                           concat (domain, table2.domain), key, rows);
     } // join
 
     /************************************************************************************
      * Join this table and table2 by performing a "theta-join".  Tuples from both tables
      * are compared attribute1 <op> attribute2.  Disambiguate attribute names by appending "2"
      * to the end of any duplicate attribute name.  Implement using a Nested Loop Join algorithm.
      *
      * #usage movie.join ("studioName == name", studio)
      *
      * @param condition  the theta join condition
      * @param table2     the rhs table in the join operation
      * @return  a table with tuples satisfying the condition
      */
     public Table join (String condition, Table table2)
     {
         out.println ("RA> " + name + ".join (" + condition + ", " + table2.name + ")");
         var rows    = new ArrayList <Comparable []> ();
 
         var eq = condition.split(" ");
         String attribute1 = eq[0];
         String attribute2 = eq[2];

         String opp = eq[1];

         int att1Index = col(attribute1);
         int att2Index = table2.col(attribute2);
 
         for (var tupleU : tuples) {
            for (var tupleT : table2.tuples) {
                boolean valid = false;
                switch (opp){
                    case "==": valid = tupleU[att1Index].compareTo(tupleT[att2Index]) == 0;
                    case "!=": valid = tupleU[att1Index].compareTo(tupleT[att2Index]) !=  0;
                    case "<" : valid = tupleU[att1Index].compareTo(tupleT[att2Index]) <  0;
                    case "<=": valid = tupleU[att1Index].compareTo(tupleT[att2Index]) <=  0;
                    case ">" : valid = tupleU[att1Index].compareTo(tupleT[att2Index]) > 0;
                    case ">=": valid = tupleU[att1Index].compareTo(tupleT[att2Index]) >= 0;
                    default: valid = false;
                }

                if (valid) {
                    var row = new Comparable[attribute.length + table2.attribute.length];
                    for (int i = 0; i < attribute.length; i++) {
                        row[i] = tupleU[i];
                    }

                    for (int i = 0; i < table2.attribute.length; i++) {
                        row[attribute.length + i] = tupleU[i];
                    }

                    rows.add(row);
                }
            }
         }
 
         return new Table (name + count++, concat (attribute, table2.attribute),
                                           concat (domain, table2.domain), key, rows);
     } // join
 
     /************************************************************************************
      * Join this table and table2 by performing an "equi-join".  Same as above equi-join,
      * but implemented using an INDEXED JOIN algorithm.
      *
      * @param attributes1  the attributes of this table to be compared (Foreign Key)
      * @param attributes2  the attributes of table2 to be compared (Primary Key)
      * @param table2       the rhs table in the join operation
      * @return  a table with tuples satisfying the equality predicate
      */
     public Table i_join (String attributes1, String attributes2, Table table2)
     {
         out.println ("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", " + table2.name + ")");

        System.out.println("Using optimized Lin_Hashmap Indexed Join!");
 
        var t_attrs = attributes1.split (" ");
        var u_attrs = attributes2.split (" ");

        var t_attrsIndexes = match(t_attrs);
        var u_attrsIndexes = table2.match(u_attrs);

        int numAttributes = t_attrs.length;

        // USES LIN HASH MAP TO OPTIMIZE THE I_JOIN METHOD
        LinHashMap <KeyType, Comparable[]> indexing = new LinHashMap<>(KeyType.class, Comparable[].class);

        // finds necessary index of primary key in table
        // this will be used in indexing hashmap
        int indexOfK = -1;
        for (int i = 0; i < table2.attribute.length; i++) {
            if (table2.attribute[i].equals(attributes2)) {
                indexOfK = i;
            }
        }

        // uses lin hash_map to keep track of which elements are inside the correct table
        indexing.create_index(this.attribute[indexOfK]);
        // adds element to indexing hashmap to keep track of which elements
        for (KeyType keyTOne: this.index.keySet()) {
            if (keyTOne != null) {
                indexing.put(keyTOne, this.index.get(keyTOne));
            } //if 
        }

        // rows to be added
        var rows = new ArrayList <Comparable []> ();

        for (Comparable[] tupleT : tuples) { for (Comparable[] tupleU : table2.tuples) {
            boolean valid = true;

            for (int i = 0; i < numAttributes; i++) {
                if (tupleT[t_attrsIndexes[i]].compareTo(tupleU[u_attrsIndexes[i]]) != 0) {
                    valid = false;
                }
                // CHECKS IF INDEX IS CONTAINED IN HASHMAP ********
                if (indexing.containsKey(tupleU[indexOfK])) {
                    valid = false;
                }
            }

            // adds index if it is seen to hashmap
            if (valid) {
                // row is populated with necessary information
                Comparable[] row = new Comparable[tupleT.length + tupleU.length];
                for (int i = 0; i < tupleT.length; i++) {
                    row[i] = tupleT[i];
                }

                for (int i = 0; i < tupleU.length; i++) {
                    row[tupleT.length + i] = tupleU[i];
                }

                rows.add(row);
            }
            }
         }

        return new Table (name + count++, concat (attribute, table2.attribute),
                                          concat (domain, table2.domain), key, rows);
     } // i_join
 
     /************************************************************************************
      * Join this table and table2 by performing an NATURAL JOIN.  Tuples from both tables
      * are compared requiring common attributes to be equal.  The duplicate column is also
      * eliminated.
      * 
      * *** PROJECT 3: IMPORTANT MODIFICATION: WITH PROJECT 3, this method allows NoMap, HashMap, LinHashMap, and TreeMap
      * which is automatically determined by which type of map is being used.
      *
      * #usage movieStar.join (starsIn)
      *
      * @param table2  the rhs table in the join operation
      * @return  a table with tuples satisfying the equality predicate
      */
     public Table join (Table table2)
     {
         out.println ("RA> " + name + ".join (" + table2.name + ")");
 
         var rows = new ArrayList <Comparable []> ();
         var duplicateColumnNames = new ArrayList <String> ();

         for (int i = 0 ; i < attribute.length; i++) {
            for (int j = 0; j < table2.attribute.length; j++) {
                if (attribute[i].equals(table2.attribute[j])) {
                    duplicateColumnNames.add(attribute[i]);
                }
            
            }
         }

        // -- IMPORTANT: MAKES NATURAL JOIN COMPATIBLE WITH INDEXING
        // *** Changes type of indexing based on the selected mode
        /*
        Map<KeyType, Comparable[]> indexing;
        switch (this.mType) {
        case LINHASH_MAP:
            indexing = new LinHashMap<>(KeyType.class, Comparable[].class);
            break;
        case HASH_MAP:
            indexing = new HashMap<KeyType, Comparable[]>();
            break;
        case TREE_MAP:
            indexing = new TreeMap<KeyType, Comparable[]>();
            break;
        default:
            // NO MAP case of indexing
            indexing = null; // this gets checked everytime indexing is potentially used
            break;
        }
        */

        // accounts for duplicate columns that may arise with indexing
         var duplicateColumnNamesArray = new String[duplicateColumnNames.size()];
         for (int i = 0; i < duplicateColumnNames.size(); i++) {
            duplicateColumnNamesArray[i] = duplicateColumnNames.get(i);
         }

         var duplicateColumnIndexesT = match(duplicateColumnNamesArray);
         var duplicateColumnIndexesU = table2.match(duplicateColumnNamesArray);

         var nonDuplicateColumnIndexesT = new Integer[attribute.length - duplicateColumnIndexesT.length];
         var nonDuplicateColumnIndexesU = new Integer[table2.attribute.length - duplicateColumnIndexesU.length];

         int numColumnsAdded = 0;
         for (int i = 0; i < attribute.length; i++) {
            boolean valid = true;
            for (int j = 0; j < duplicateColumnIndexesT.length; j++) {
                if (i == duplicateColumnIndexesT[j]) valid = false;
            }
            if (valid) {
                nonDuplicateColumnIndexesT[numColumnsAdded] = i;
                numColumnsAdded++;
            }
         }

         numColumnsAdded = 0;
         for (int i = 0; i < table2.attribute.length; i++) {
            boolean valid = true;
            for (int j = 0; j < duplicateColumnIndexesU.length; j++) {
                if (i == duplicateColumnIndexesU[j]) valid = false;
            }
            if (valid) {
                nonDuplicateColumnIndexesU[numColumnsAdded] = i;
                numColumnsAdded++;
            }
         }

         int numDuplicates = duplicateColumnNames.size();


         // **** performs join using indexing if applicable
         for (Comparable[] tupleT : tuples) {
            for (Comparable[] tupleU : table2.tuples) {
                boolean valid = true;

                for (int i = 0; i < numDuplicates; i++) {
                    if (tupleT[duplicateColumnIndexesT[i]].compareTo(tupleU[duplicateColumnIndexesU[i]]) != 0) {
                        valid = false;
                    }
                }
                
                /* 
                // IMPORTANT
                // checks to see if INDEXING is being used
                if (indexing != null) {
                    // uses indexing to speed up validity condition
                    if (indexing.get(tupleT[0]) != null) {
                        valid = false;
                    }
                }
                */

                if (valid) {
                    Comparable[] row = new Comparable[tupleT.length + tupleU.length - numDuplicates];
                    int numAttributesAdded = 0;
                    for (int i = 0; i < attribute.length; i++) {
                        row[i] = tupleT[i];
                        numAttributesAdded += 1;
                    }

                    for (int i = 0; i < nonDuplicateColumnIndexesU.length; i++) {
                        row[numAttributesAdded + i] = tupleU[nonDuplicateColumnIndexesU[i]];
                    }

                    rows.add(row);
                    // us
                }
            }
         }

         String newAttributes[] = new String[attribute.length + table2.attribute.length - numDuplicates];
         Class newDomain[] = new Class[attribute.length + table2.attribute.length - numDuplicates];
         for (int i = 0; i < attribute.length; i++) {
            newAttributes[i] = attribute[i];
            newDomain[i] = domain[i];
         }

         for (int i = 0; i < nonDuplicateColumnIndexesU.length; i++) {
            newAttributes[attribute.length + i] = table2.attribute[nonDuplicateColumnIndexesU[i]];
            newDomain[attribute.length + i] = table2.domain[nonDuplicateColumnIndexesU[i]];
         }

         return new Table (name + count++, newAttributes,
                                           newDomain, key, rows);
     } // join
 
     /************************************************************************************
      * Return the column position for the given attribute name or -1 if not found.
      *
      * @param attr  the given attribute name
      * @return  a column position
      */
     public int col (String attr)
     {
         for (var i = 0; i < attribute.length; i++) {
            if (attr.equals (attribute [i])) return i;
         } // for
 
         return -1;       // -1 => not found
     } // col
 
     /************************************************************************************
      * Insert a tuple to the table.
      *
      * #usage movie.insert ("Star_Wars", 1977, 124, "T", "Fox", 12345)
      *
      * @param tup  the array of attribute values forming the tuple
      * @return  the insertion position/index when successful, else -1
      */
     public int insert (Comparable [] tup)
     {
         out.println ("DML> insert into " + name + " values (" + Arrays.toString (tup) + ")");
 
         if (typeCheck (tup)) {
             tuples.add (tup);
             var keyVal = new Comparable [key.length];
             var cols   = match (key);
             for (var j = 0; j < keyVal.length; j++) keyVal [j] = tup [cols [j]];
             if (mType != MapType.NO_MAP) index.put (new KeyType (keyVal), tup);
             return tuples.size () - 1;                             // assumes it is added at the end
         } else {
             return -1;                                             // insert failed
         } // if
     } // insert
 
     /************************************************************************************
      * Get the tuple at index position i.
      *
      * @param i  the index of the tuple being sought
      * @return  the tuple at index position i
      */
     public Comparable [] get (int i)
     {
         return tuples.get (i);
     } // get
 
     /************************************************************************************
      * Get the name of the table.
      *
      * @return  the table's name
      */
     public String getName ()
     {
         return name;
     } // getName
 
     /************************************************************************************
      * Print tuple tup.
      * @param tup  the array of attribute values forming the tuple
      */
     public void printTup (Comparable [] tup)
     {
         out.print ("| ");
         for (var attr : tup) out.printf ("%15s", attr);
         out.println (" |");
     } // printTup
 
     /************************************************************************************
      * Print this table.
      */
     public void print ()
     {
         out.println ("\n Table " + name);
         out.print ("|-");
         out.print ("---------------".repeat (attribute.length));
         out.println ("-|");
         out.print ("| ");
         for (var a : attribute) out.printf ("%15s", a);
         out.println (" |");
         out.print ("|-");
         out.print ("---------------".repeat (attribute.length));
         out.println ("-|");
         for (var tup : tuples) printTup (tup);
         out.print ("|-");
         out.print ("---------------".repeat (attribute.length));
         out.println ("-|");
     } // print
 
     /************************************************************************************
      * Print this table's index (Map).
      */
     public void printIndex ()
     {
         out.println ("\n Index for " + name);
         out.println ("-------------------");
         if (mType != MapType.NO_MAP) {
             for (var e : index.entrySet ()) {
                 out.println (e.getKey () + " -> " + Arrays.toString (e.getValue ()));
             } // for
         } // if
         out.println ("-------------------");
     } // printIndex
 
     /************************************************************************************
      * Load the table with the given name into memory. 
      *
      * @param name  the name of the table to load
      */
     public static Table load (String name)
     {
         Table tab = null;
         try {
             ObjectInputStream ois = new ObjectInputStream (new FileInputStream (DIR + name + EXT));
             tab = (Table) ois.readObject ();
             ois.close ();
         } catch (IOException ex) {
             out.println ("load: IO Exception");
             ex.printStackTrace ();
         } catch (ClassNotFoundException ex) {
             out.println ("load: Class Not Found Exception");
             ex.printStackTrace ();
         } // try
         return tab;
     } // load
 
     /************************************************************************************
      * Save this table in a file.
      */
     public void save ()
     {
         try {
             var oos = new ObjectOutputStream (new FileOutputStream (DIR + name + EXT));
             oos.writeObject (this);
             oos.close ();
         } catch (IOException ex) {
             out.println ("save: IO Exception");
             ex.printStackTrace ();
         } // try
     } // save
 
     //----------------------------------------------------------------------------------
     // Private Methods
     //----------------------------------------------------------------------------------
 
     /************************************************************************************
      * Determine whether the two tables (this and table2) are compatible, i.e., have
      * the same number of attributes each with the same corresponding domain.
      *
      * @param table2  the rhs table
      * @return  whether the two tables are compatible
      */
     private boolean compatible (Table table2)
     {
         if (domain.length != table2.domain.length) {
             out.println ("compatible ERROR: table have different arity");
             return false;
         } // if
         for (var j = 0; j < domain.length; j++) {
             if (domain [j] != table2.domain [j]) {
                 out.println ("compatible ERROR: tables disagree on domain " + j);
                 return false;
             } // if
         } // for
         return true;
     } // compatible
 
     /************************************************************************************
      * Match the column and attribute names to determine the domains.
      *
      * @param column  the array of column names
      * @return  an array of column index positions
      */
     private int [] match (String [] column)
     {
         int [] colPos = new int [column.length];
 
         for (var j = 0; j < column.length; j++) {
             var matched = false;
             for (var k = 0; k < attribute.length; k++) {
                 if (column [j].equals (attribute [k])) {
                     matched = true;
                     colPos [j] = k;
                 } // for
             } // for
             if ( ! matched) out.println ("match: domain not found for " + column [j]);
         } // for
 
         return colPos;
     } // match
 
     /************************************************************************************
      * Extract the attributes specified by the column array from tuple t.
      *
      * @param t       the tuple to extract from
      * @param column  the array of column names
      * @return  a smaller tuple extracted from tuple t 
      */
     private Comparable [] extract (Comparable [] t, String [] column)
     {
         var tup    = new Comparable [column.length];
         var colPos = match (column);
         for (var j = 0; j < column.length; j++) tup [j] = t [colPos [j]];
         return tup;
     } // extract
 
     /************************************************************************************
      * Check the size of the tuple (number of elements in array) as well as the type of
      * each value to ensure it is from the right domain. 
      *
      * @param t  the tuple as a array of attribute values
      * @return  whether the tuple has the right size and values that comply
      *          with the given domains
      */
     private boolean typeCheck (Comparable [] t)
     { 
          return t.length == this.domain.length; 
     } // typeCheck
 
     /************************************************************************************
      * Find the classes in the "java.lang" package with given names.
      *
      * @param className  the array of class name (e.g., {"Integer", "String"})
      * @return  an array of Java classes
      */
     private static Class [] findClass (String [] className)
     {
         var classArray = new Class [className.length];
 
         for (var i = 0; i < className.length; i++) {
             try {
                 classArray [i] = Class.forName ("java.lang." + className [i]);
             } catch (ClassNotFoundException ex) {
                 out.println ("findClass: " + ex);
             } // try
         } // for
 
         return classArray;
     } // findClass
 
     /************************************************************************************
      * Extract the corresponding domains.
      *
      * @param colPos  the column positions to extract.
      * @param group   where to extract from
      * @return  the extracted domains
      */
     private Class [] extractDom (int [] colPos, Class [] group)
     {
         var obj = new Class [colPos.length];
 
         for (var j = 0; j < colPos.length; j++) {
             obj [j] = group [colPos [j]];
         } // for
 
         return obj;
     } // extractDom
 
 } // Table
 
