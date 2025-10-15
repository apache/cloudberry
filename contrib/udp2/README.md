<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# UDP2 Interconnect Protocol Implementation

## Project Background

UDP2 is a next-generation interconnect protocol implementation based on the original UDP protocol, located in the `contrib/udp2` directory. In CloudBerry Database, the interconnect is responsible for data transmission and synchronization between nodes, serving as a core component for distributed query execution.

Currently, the database supports three interconnect protocol implementations:
- **TCP** (`contrib/interconnect/tcp`) - Reliable transmission based on TCP protocol
- **UDP** (`contrib/interconnect/udp`) - High-performance transmission based on UDP protocol
- **Proxy** (`contrib/interconnect/proxy`) - Proxy-based transmission

UDP2 is an architectural refactoring based on the original UDP protocol implementation, aimed at achieving complete separation between interconnect and the database kernel.

## Project Goals

The core objectives of the UDP2 protocol implementation are:

1. **Architecture Decoupling**: Completely separate the interconnect protocol implementation from the database kernel, enabling independent development and evolution
2. **Independent Testing**: Enable end-to-end functional and performance testing of interconnect without depending on the database kernel
3. **Rapid Diagnosis**: Quickly identify whether issues are at the interconnect level or database kernel level
4. **Modular Design**: Provide clear interface boundaries for easier extension and maintenance

## Current Project Implementation Architecture

### Overall Architecture Design

UDP2 adopts a layered architecture design, primarily divided into two layers:

```
┌─────────────────────────────────────────────────────────┐
│                Database Kernel Layer                    │
│  ┌────────────────────────────────────────────────────┐ │
│  │            contrib/udp2/                           │ │
│  │  ┌─────────────────┐  ┌────────────────────────┐   │ │
│  │  │   ic_modules.c  │  │      ic_udp2.c         │   │ │
│  │  │   ic_modules.h  │  │      ic_udp2.h         │   │ │
│  │  └─────────────────┘  └────────────────────────┘   │ │
│  │              Adapter Layer (Database Adapter)      │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
                              │
                              │ C/C++ Interface
                              ▼
┌──────────────────────────────────────────────────────────┐
│            Independent IC Communication Library          │
│  ┌─────────────────────────────────────────────────────┐ │
│  │            contrib/udp2/ic_common/                  │ │
│  │  ┌─────────────────┐  ┌─────────────────────────┐   │ │
│  │  │   ic_types.h    │  │      ic_utility.hpp     │   │ │
│  │  │   ic_except.hpp │  │   ic_faultinjection.h   │   │ │
│  │  └─────────────────┘  └─────────────────────────┘   │ │
│  │  ┌────────────────────────────────────────────────┐ │ │
│  │  │         contrib/udp2/ic_common/udp2/           │ │ │
│  │  │  ┌─────────────────┐  ┌─────────────────────┐  │ │ │
│  │  │  │   ic_udp2.h     │  │   ic_udp2.hpp       │  │ │ │
│  │  │  │   ic_udp2.cpp   │  │ic_udp2_internal.hpp │  │ │ │
│  │  │  └─────────────────┘  └─────────────────────┘  │ │ │
│  │  └────────────────────────────────────────────────┘ │ │
│  │              Core Communication Library             │ │
│  └─────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

### Core Component Description

#### 1. Adapter Layer (`contrib/udp2/`)
- **ic_modules.c/h**: Module registration and initialization, implementing the `MotionIPCLayer` interface
- **ic_udp2.c/h**: Adapter layer between database kernel and ic_common library
- Responsible for converting database kernel data structures to ic_common library standard interfaces

#### 2. Core Communication Library (`contrib/udp2/ic_common/`)
- **ic_types.h**: Defines core data types and interfaces, decoupled from database kernel
- **ic_utility.hpp**: Common utility functions and logging system
- **ic_except.hpp**: Exception handling mechanism
- **ic_faultinjection.h**: Fault injection testing support

#### 3. UDP2 Protocol Implementation (`contrib/udp2/ic_common/udp2/`)
- **ic_udp2.h**: C language interface definition
- **ic_udp2.hpp**: C++ interface definition
- **ic_udp2.cpp**: Core protocol implementation
- **ic_udp2_internal.hpp**: Internal implementation details

### Build System

UDP2 uses CMake build system with support for independent compilation:

```
contrib/udp2/
├── CMakeLists.txt         # Main build configuration
├── Makefile               # PostgreSQL-compatible Makefile
└── ic_common/
    ├── CMakeLists.txt     # ic_common library build configuration
    └── build/             # Build output directory
```

Build process:
1. First build the `ic_common` dynamic library (`libic_common.so`)
2. Then build the `udp2` module (`udp2.so`), linking against the `ic_common` library

## How to Switch Database to This Protocol Implementation

### Enable UDP2 Support at Compile Time

1. **Configure compilation options**:
```bash
./configure --enable-ic-udp2 [other options]
make && make install
```

2. **Verify compilation results**:
```bash
# Check if udp2.so is generated
ls -la $GPHOME/lib/postgresql/udp2.so

# Check if ic_common library is installed
ls -la $GPHOME/lib/libic_common.so
```

### Runtime Configuration

```bash
# Set cluster to use UDP2 by default
gpconfig -c gp_interconnect_type -v udp2

# Reload configuration
gpstop -air
```

```sql
-- Check current interconnect type
SHOW gp_interconnect_type;
```

## Technical Details

### Interface Design

UDP2 achieves decoupling between database kernel and communication library through standardized C interfaces:

```c
// Core interface functions (ic_common/udp2/ic_udp2.h)
extern ICChunkTransportState* UDP2_SetupUDP(ICSliceTable *sliceTable,
                                            SessionMotionLayerIPCParam *param);
extern void UDP2_TeardownUDP(ICChunkTransportState *transportStates, bool hasErrors);

// Data send/receive interfaces
extern bool UDP2_SendData(ICChunkTransportState *transportStates,
                          int16 motNodeID, int16 targetRoute,
                          DataBlock *pblocks, int num, bool broadcast);
extern void UDP2_RecvAny(ICChunkTransportState *transportStates,
                         int16 motNodeID, int16 *srcRoute,
                         GetDataLenInPacket getLen, DataBlock *data);
```

### Data Structure Mapping

UDP2 defines lightweight data structures to replace complex database kernel structures:

```c
// Lightweight process information (replaces CdbProcess)
typedef struct ICCdbProcess {
    bool valid;
    char *listenerAddr;
    int listenerPort;
    int pid;
    int contentid;
    int dbid;
} ICCdbProcess;

// Lightweight slice information (replaces ExecSlice)
typedef struct ICExecSlice {
    int sliceIndex;
    int parentIndex;
    int numChildren;
    int *children;
    int numSegments;
    int numPrimaryProcesses;
    ICCdbProcess *primaryProcesses;
} ICExecSlice;
```

### Error Handling Mechanism

UDP2 implements a unified error handling mechanism:

```c
typedef enum ErrorLevel {
    LEVEL_OK,
    LEVEL_ERROR,
    LEVEL_FATAL,
} ErrorLevel;

// Error handling interfaces
extern void SetLastError(ErrorLevel level, const char *msg);
extern ICError* GetLastError();
extern void ResetLastError();
```

## Development and Debugging

### Independent Compilation Testing

```bash
# Enter ic_common directory
cd contrib/udp2/ic_common

# Create build directory
mkdir build && cd build

# Configure and compile
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j
```

### Debug Configuration

Enable verbose logging in development environment:

```sql
-- Enable interconnect debug logging
SET gp_log_interconnect = 'debug';

-- Set log level
SET log_min_messages = 'debug1';

-- Enable detailed error information
SET gp_interconnect_log_stats = on;
```
