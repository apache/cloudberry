# Extend protocol of libpq

## Overview

In certain scenarios, the Query Executor (QE) needs to send messages back to the Query Dispatcher (QD) during execution. For instance, it may need to record and communicate modified partition child tables during data modification processes. To facilitate this, the libpq protocol has been extended to support message transfer through an 'e' protocol, which stands for "extend protocol." This extension unifies the handling of information returns.

## Data Structure

### ExtendProtocolData

All data returned from extended protocols will be stored in the `ExtendProtocolData` structure. Each time libpq returns data, the QD will parse it into `subtag` data, which will be stored in corresponding linked lists. Each `subtag` will have its own linked list to manage the associated data blocks.

### Data Block Layout

The layout for each transmission under the extended protocol is as follows:

```
'e' + data_block_0 + ... + data_block_n + subtag_max
```

#### Data Block Structure

Each `data_block` has the following structure:

```
subtag + length(int32) + binary_data
```

- **subtag**: Identifies the type of data.
- **length (int32)**: Specifies the total length of the subsequent `binary_data`.
- **binary_data**: The actual data being transmitted.

### Transmission Process

1. Each transmission begins with the 'e' protocol, followed immediately by one or more data blocks.
2. Each data block corresponds to a specific type of data, distinguished by its `subtag`.
3. After all data blocks have been written, the transmission concludes with a `subtag_max`, which indicates the termination of the extended protocol transmission.

## Data Management

### Storage and Lifecycle

- The `ExtendProtocolData` will store data until it is consumed or until the top-level transaction ends.
- Consumed data will be marked as successfully consumed and removed from storage.
- If there is any data that remains unconsumed by the time the top-level transaction commits, a warning will be printed.
- Upon transaction termination, all stored data will be cleared.

### Consume Extend Protocol Data

Each time the QD receives an extended protocol message, it processes the subtags and stores the corresponding `binary_data` into the `subtagdata` list of `ExtendProtocolData`. During this process, the subtag is marked with a status indicating it is ready to be consumed. The stored data is allocated under the `TopTransactionContext`. 

No processing or error handling occurs at this stage; it is solely focused on data storage. To consume the stored data by subtag, the caller should use the `ConsumeExtendProtocolData()` function. This function will mark the subtag as consumed and clean up the associated `subtagdata`. If the function is called multiple times without receiving additional extended protocol messages since the last call, there will be no data available for consumption.
