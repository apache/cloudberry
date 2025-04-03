# PAX toast

## Background

The toast aims to address several issues related to handling huge tuples in the PAX storage format.

1. If a single tuple is huge, only a small amount of data can be stored in a PAX file.
2. Without external storage, OSS(Object Storage Service) is inconvenient to retrieve data.
3. Once a single tuple is huge, the data in min/max will also be huge, which will increase the risk of OOM.


In Cloudberry, toast is used to store relatively large tuples, and the `make_toast` size depends on `TOAST_TUPLE_THRESHOLD`.

PAX must be thread-safe. Directly using the TOAST table to store TOAST data is not thread-safe. And if we use `toast_tuple_init/toast_tuple_try_compression` and other interfaces, it is also non-thread safe. 

If the TOAST table is not used, then when a table has toast data, it will not be inserted into the table `pg_toast.pg_toast_<reltoastrelid>`.

# Implementation

PAX supports two kinds of toasts

1. `compress toast`: the structure is consistent with Cloudberry's implementation
2. `external toast`: PAX customization

Unlike Cloudberry, the external toast only exists on disk and it can't be used in memory.

- For reading and writing, PAX will customize the area to correspond
- Structurally, the varlena head is extended to add a new type, but the Cloudberry is unaware of it.

For the compress toast, we can directly write or read it in the tuple
- In terms of reading and writing, tuple is passed in when writing, or PAX generates compress toast by itself, and no detoast operation is performed after reading.
- Structurally, it is consistent with Cloudberry's implementation

For toast operation methods: PAX no longer reuses the methods in detoast.h/heaptoast.h, but writes its own set of operators.


## Compress toast


Compress toast is consistent with Cloudberry compress toast.

This part of the data is a varlena structure, and it is of type `varattrib_4b`, which means that it has a varlena head and the storage range is less than 1G.

In addition, the lower limit of the datum length that needs to be compressed on PAX may be set to larger, and TOAST_TUPLE_THRESHOLD is no longer used as the threshold. Instead, we added a GUC(`pax_min_size_of_compress_toast`) to use.

```
typedef union
{
    struct                                                
    {
        uint32      va_header;
        char        va_data[FLEXIBLE_ARRAY_MEMBER];
    } va_4byte;
    
    struct
    {
        uint32      va_header;
        uint32      va_rawsize; /* origin size */
        char        va_data[FLEXIBLE_ARRAY_MEMBER]; 
    } va_compressed;
    
} varattrib_4b;

---------------------------------------------------
|    tag    |    compress    |      length        |
---------------------------------------------------
|   1 bit   |    1 bit       |      30 bit        |
---------------------------------------------------
```


## External toast

### Structure

External toast is no longer structurally identical to Cloudberry external toast.

The current Cloudberry external toast structure is `varattrib_1b_e`, which the data part is divided into two types
- The data is in the disk, then the data part is `varatt_external`
- The data is in the memory, then the data part is `varatt_expanded`

```
typedef struct
{
   uint8    va_header;     
   uint8    va_tag;        /* type */
   char     va_data[FLEXIBLE_ARRAY_MEMBER];
} varattrib_1b_e;
```

However, the structure of `varattrib_1b_e` still be reused , so that PAX can determine the current toast through the `va_tag` type

So we defined a custom TAG(`VARTAG_CUSTOM`) in the Cloudberry to fill in `va_tag`.

The part of `va_data`:

```
typedef struct {
  int32 va_rawsize;  /* Original data size (doesn't include header) */
  uint32 va_extinfo; /* External saved size (without header) and
                      * compression method */
  uint64 va_extogsz; /* The origin size of external toast */
  uint64 va_extoffs; /* The offset of external toast */
  uint64 va_extsize; /* The size of external toast */
} pax_varatt_external;
```

### External toast in memory

External TOAST data is not stored as a datum within the tuple. Because the exector doesn't know how to detoast the PAX's external toast. So in PAX, we need to make sure the external has been detoasted once the current column has been read.

We added a buffer in `PaxColumn`. Each column can use this buffer(named `external buffer`) to access its own external toast.

The pointer (`va_extoffs` + `va_extsize`) in the external toast will point to the location of its own external buffer (not the location of the save)

- When writing, the external buffer of the column will be collected according to the column index. If it does not exist, it will be skipped to form a large buffer and written to the disk.
- When reading, the external buffer corresponding to the column on the disk will be restored to the memory through column projection, so that the external toast directly points to the corresponding location.

### toast file

The external toast won't store column data but use a `.toast` file to store it.

- The toast file is use to store external toast data
- The organization method is by column index as the order, and the raw data is stored directly
  - This is convenient for merging io during column projection
- Several fields describing external toast are added to `StripeInformation` (metadata part describing group)
  - `toastOffset`: the starting offset of the external toast existing in the current group in the toast file
  - `toastLength`: the length of the external toast existing in the current group in the toast file
  - `numberOfToast`: How many toasts are there in the current group (compress toast is also included here)
  - `repeated extToastLength`: the length of the external toast of each column in the current group, used to handle column projections


## Storage Type 

PAX determines whether to generate TOAST data based on the PostgreSQL [storage type](https://www.postgresql.org/docs/current/storage-toast.html).

This is consistent with the heap table.

