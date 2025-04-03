# PAX filter

## Overview

PAX filter is used for pre-filtering. In Cloudberry, there are push-down filtering and column projection. Unnecessary data can be filtered out before PAX reads the files.

There are three types of filtering in PAX:

1. Column projection
2. Sparse filtering
3. Row filtering

## Statistics

Before we look at filtering, let's look at the statistics provided by PAX. Note that statistics do not affect column projections.

There are three types of statistics in PAX: basic stats (containing `isnull` and `allnull`), min/max and bloom filter. When creating a PAX table, users can specify which columns require which type of statistics through reloptions. Statistics are generated when writing, when users specify that some columns require statistics, it will slow down writing.

In PAX, there are statistics for both files and groups within files. When writing a PAX file, the statistics will not counted twice. Instead, after the group statistics are generated, they will be merged together to generate file-level statistics. When reading a pax file, if file-level statistics are not effective, group-level statistics may still be effective.

The relevant protobuf definitions are in `micro_partition_stats.proto`, and the implementation is in `micro_partition_stats.cc`.

### MIN/MAX

MIN/MAX statistics will record the maximum and minimum values ​​of the current column. Since the min/max comparison operators are overloaded in PAX (the reason for overloading is that PAX needs to support thread safety), not all types support min/max.

MIN/MAX are useful for operators such as `</>/=/<=/>=`. For example, if the min/max range of the current column is [0, 9] in current PAX file, and the filtering condition is a > 10. Then the current file does not need to be read again because there is no row that meets the condition.

The structure of min/max statistics:

```
message ColumnDataStats {
  optional bytes minimal = 1;             // Minimal value stored as Datum.
  optional bytes maximum = 2;             // Maximum value stored as Datum.
  optional bytes sum = 3;                 // sum(c1) value. Invalid after existence of visibility map
}
```

The `sum` field will not be used to do the filter but it can do the `CustomScan`(not defined in Cloudberry).

### Bloom filter

Bloom filter is a data structure used to quickly determine whether an element may exist in a set. Its core is implemented through a bit array and multiple hash functions: when adding an element, the element is hashed multiple times and the corresponding bit is marked as 1; when querying, if all hash bits are 1, it is determined that the element may exist (there may be a misjudgment), otherwise it must not exist.

The structure of bloom filter statistics:

```
message BloomFilterBasicInfo {
  optional int32  bf_hash_funcs = 1;      // the number of hash functions
  optional uint64 bf_seed = 2;            // the seed of bloom filter
  optional uint64 bf_m = 3;               // the bits of bit sets in the bloom filter
}

message ColumnStats {
  ...
  optional bytes columnBFStats = 7;
}
```

The basic information of bloom filter needs to be retained in protobuf, because only bloom filter constructed in the same way can be compared. And we use a bytes array to represent the constructed bloom filter.

## Column projection

Column projection refers to the operation of selecting specific columns (attributes) from a dataset or database table while discarding others. It reduces the data volume by focusing only on relevant fields, improving query efficiency and simplifying analysis.  

In PAX, column projection is achieved by skipping the streaming(pb streaming) of the data module. more details in `ReadStripe()`.

## Sparse filtering

The implementation of sparse filter in PAX is divided into two parts:
1. Convert different executor(Cloudberry and vectorization) expressions into the intermediate format filter tree defined by PAX.
   1.1. The Cloudberry expression conversion logic in `pax_sparse_pg_path.cc`. it will use the `Expr *` build the `std::shared_ptr<PFTNode>`.
   1.2. The vectorization expression conversion logic in `pax_sparse_vec_path.cc`. it will use the `const arrow::compute::Expression &` build the `std::shared_ptr<PFTNode>`.
2. Recursively execute the filter tree.
   2.1. Bottom-up recursive execution
   2.2. If the current subtree meets missing statistics or unsupported expressions, the subtree will be abandoned. Other subtrees will still be executed.


The basic structure of the sparse filtering execution node is:

```
struct PFTNode {  // PTF = PAX filter tree
  PFTNodeType type; // The type of current node
  std::shared_ptr<PFTNode> parent;  // parent node
  int subidx;                       // the position in parent node
  std::vector<std::shared_ptr<PFTNode>> sub_nodes; // The subnodes

  PFTNode() : type(InvalidType), parent(nullptr), subidx(-1) {}
  PFTNode(PFTNodeType t) : type(t), parent(nullptr), subidx(-1) {}

  static inline void AppendSubNode(const std::shared_ptr<PFTNode> &p_node,
                                   std::shared_ptr<PFTNode> &&sub_node);

  virtual ~PFTNode() = default;

  virtual std::string DebugString() const { return "Empty Node(Invalid)"; }
};

```

Currently PAX supports the following filter expressions:

- Arithmetic operators (OpExpr)
  - Supports addition (`+`), subtraction (`-`), and multiplication (`*`)
  - Division operations are not supported (due to difficulty in estimating negative number ranges)
  - Examples: `a + 1, 1 - a, a * b`
- Comparison operators (OpExpr)
  - Supports `<`, `<=`, `=`, `>=`, `>`
  - Examples: `a < 1`, `a + 1 < 10`, `a = b`
- Logical operators (BoolExpr)
  - Supports `AND`, `OR`, `NOT`
  - Example: `a < 10 AND b > 10`
- NULL value tests (NullTest)
  - Supports `IS NULL`, `IS NOT NULL`
  - Examples: `a IS NULL`, `a IS NOT NULL`
- Type casting (FuncExpr)
  - Only supports `basic type` casting
  - Example: `a::float8 < 1.1`
  - Custom functions are not supported, such as `func(a) < 10`
- IN operator (ScalarArrayOpExpr)
  - Supports `IN`, `NOT IN`
  - Example: `a IN (1, 2, 3)`

## Row filtering

Row filtering is mainly used in the Cloudberry executor. The Cloudberry executor is a row-based executor. Row filtering works poorly for a column-based executor.

The case of Row filtering:  Target-list in `SeqScan` is big, but only some columns have filtering conditions. And a large amount of data can be filtered through the filtering conditions.

For example:

```
create table t1(v1 int, v2 int, v3 int, ...  ,v100 int); -- 100 column

select * from t1 where v1 < 100 and v2 < 200;
```

In the current example, we need to return 100 columns, and the filter condition is added to the first two columns. If the row filter is enabled, 

- PAX will read the first two columns.
- Use the row filter to determine which rows should be taken from the last 98 columns.




