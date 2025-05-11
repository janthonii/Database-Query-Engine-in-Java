# Relational Database Engine with Performance Benchmarking
**Completed as apart of CSCI 4370: Database Management**

A complete Java implementation of a relational database system featuring:
- Core relational algebra operations
- Multiple indexing strategies
- Tuple generation for testing
- Performance benchmarking framework
- Persistent storage capabilities

## Project Structure
```
.
├── store/ # Data storage directory
├── KeyType.java # Key handling for composite/non-composite keys
├── LinHashMap.java # Linear Hashing implementation
├── Table.java # Core relational operations and table management
├── MovieDB.java # Sample movie database (creation/saving)
├── MovieDB2.java # Movie database loading demo
├── TupleGenerator.java # Tuple generation interface
├── TupleGeneratorImpl.java # Tuple generation implementation
├── TestTupleGenerator.java # Tuple generation tests
├── UniversityDB.java # University database creation
├── UniversityDBQuery.java # Performance benchmarking
└── README.md
```

## Key Features

### Core Database Operations
- **Relational Algebra**:
  - Select (with conditions)
  - Project (column selection)
  - Union/Minus (set operations)
  - Join (equi-join, natural join)
- **Table Management**:
  - Insert/update/delete records
  - Save/load tables to disk
  - Primary/foreign key support

### Indexing System
- **Multiple Index Types**:
  - Linear Hashing (LinHashMap)
  - TreeMap
  - HashMap
  - No indexing option
- **Index Operations**:
  - `create_index()`/`create_unique_index()`
  - `drop_index()`
  - Automatic index maintenance

### Performance Benchmarking
- **Query Execution Timing**:
  - Measures performance across 6 iterations
  - Discards JIT warmup (first iteration)
  - Calculates average execution time
- **Test Scenarios**:
  - Strong/medium/weak filter predicates
  - Indexed vs. non-indexed comparisons
  - Scalability testing with large datasets

### Data Generation
- **TupleGenerator**:
  - Creates test data with referential integrity
  - Configurable dataset sizes
  - Supports complex relational schemas

## How to Use
**Benchmarking Options**

Configure in `Table.java`:

```java
// Change this to test different indexing strategies
static final MapType mType = MapType.LINHASH_MAP;  // Options: NO_MAP, TREE_MAP, HASH_MAP, LINHASH_MAP
```
**Tuple Generation**
Modify in UniversityDB.java:

```java
// Adjust these numbers to change dataset sizes
var tups = new int [] { 20000, 40000, 60000 };
```
