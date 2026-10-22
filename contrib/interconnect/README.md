# Intercontect 

This subtree contains interconnect module && test && benchmark that different with other subtree inside {cbdb_src}/contrib. Other moudles are not part of the core Cloudberry system, but interconnect module split from `cdb module`, it **must be preload with Cloudberry**, otherwise Cloudberry system will not work properly.

**The interconnect module will be preloaded by default as a library.** When the compile option `--disable-preload-ic-module` is turned on, then the interconnect module will not be preloaded, then users need to add `interconnect` into guc `shared_preload_libraries`.


Benefits of Separating Interconnect Separately

- Independent & decoupling, easier to add new interconnect types
- It is more convenient to **test & debug & benchmark**.


# Intercontect interfaces and data structure

The main interface name is `MotionIPCLayer` defined in `ml_ipc.h`, there are already three implementations:
- tcp
- udpifc
- proxy 

The specific method can refer to the notes. Here is a diagram to describe the specific timing of the interface function being called.

```                                                                                                                                                           
	                                                                                                                                                                                  
	                                                                                                                                                                                  
	                                                                                                                                                                                  
	                                                                                                                                                                                  
	                                                                                                                                                                                  
	                                                                                                                                                                                  
	                                                                                                                                                                                  
	                                      ┌─────────────┐         ┌─────────────────────┐                 ┌──────────────────────┐      ┌─────────────┐                               
	                                      │  cdb init   │────────▶│ InitMotionLayerIPC  │                 │  InitMotionLayerIPC  │◀─────│  cdb init   │                               
	                                      └─────────────┘         └─────────────────────┘                 └──────────────────────┘      └─────────────┘                               
	                                                                         │                                        │                                                               
	                                                                         │              ┌──────────┐              │                                                               
	                                                                         │              │ registe  │              │                                                               
	                                                                         │              │request(if│              │                                                               
	                                                                         ▼              │  have)   │              ▼                                                               
	                                      ┌─────────────┐         ┌────────────────────┐    │          │  ┌───────────────────────┐     ┌─────────────┐                               
	                                      │ motion exec │────────▶│ SetupInterconnect  │◀───┴──────────┴──│   SetupInterconnect   │◀────│ motion exec │                               
	                                      └─────────────┘         └────────────────────┘                  └───────────────────────┘     └─────────────┘                               
	                                                                         │                                        │                                                               
	                                                                         │                                        │                                                               
	                                                                         ▼                                        ▼                                                               
	                                                              ┌────────────────────┐                  ┌──────────────────────┐                                                    
	                                                              │SendTupleChunkToAMS │                  │RecvTupleChunkFromAny/│                                                    
	                                                              └────────────────────┘                  │  RecvTupleChunkFrom  │                                                    
	   ┌───────────────────────────┐                                         │         ┌─────────────────┐└──────────────────────┘                                                    
	   │ GetTransportDirectBuffer  │───┐            ┌───────────────┐        │         │ TupleChunkList  │            │       ┌───────────────┐                                       
	   └───────────────────────────┘   │            │ internal call │──────▶ │         └─────────────────┘            │◀──────│ internal call │                                       
	                 ▲                 │            └───────────────┘        ▼                  │                     ▼       └───────────────┘                                       
	                 │                 │                          ┌────────────────────┐        ▼         ┌──────────────────────┐      ┌──────────────────┐     ┌───────────────────┐
	                 │                 │            ┌─────────────│     SendChunk      │─────────────────▶│    RecvTupleChunk    │─────▶│ udp recv buffer  │────▶│ DirectPutRxBuffer │
	      ┌────────────────────┐       │            ▼             └────────────────────┘                  └──────────────────────┘      └──────────────────┘     └───────────────────┘
	      │   direct access    │       │    ┌──────────────┐                 │             ┌────────────┐             │                                                               
	      │   reduce memcpy    │       ├───▶│ buffer Pool  │                 │             │  stop or   │             │                                                               
	      └────────────────────┘       │    └──────────────┘                 ◀─────────────│ interrupt  │─────────────▶                                                               
	                 │                 │            ▲                        │             └────────────┘             │                                                               
	                 │                 │            │                        │                    │                   │                                                               
	                 ▼                 │            │             ┌────────────────────┐          │                   ▼                           ┌──────────────────┐                
	   ┌───────────────────────────┐   │            └──────▲──────│      SendEOS       │          │       ┌──────────────────────┐                │motion exec finish│                
	   │ PutTransportDirectBuffer  │───┘        ┌──────────┘      └────────────────────┘          │       │ TeardownInterconnect │◀───────────────│   or interrupt   │                
	   └───────────────────────────┘            │                            ▲                    │       └──────────────────────┘                └──────────────────┘                
	                                      ┌──────────┐                       │                    │                   │                                                               
	                                      │  flush   │                       │                    │                   │                                                               
	                                      └──────────┘                       │                    │                   ▼                                                               
	                                                              ┌────────────────────┐          │       ┌──────────────────────┐                ┌─────────────┐                     
	                                                              │  SendStopMessage   │          │       │CleanUpMotionLayerIPC │◀───────────────│ cdb cleanup │                     
	                                                              └────────────────────┘          │       └──────────────────────┘                └─────────────┘                     
	                                                                                              │                                                                                   
	                                                                                              │                                                                                   
	                          ┌──────────────────┐                                                │                                                                                   
	                          │motion exec finish│                ┌────────────────────┐          │                                                                                   
	                          │   or interrupt   │───────────────▶│TeardownInterconnect│◀─────────┘                                                                                   
	                          └──────────────────┘                └────────────────────┘                                                                                              
	                                                                         │                                                                                                        
	                                                                         ▼                                                                                                        
	                            ┌─────────────┐                   ┌─────────────────────┐                                                                                             
	                            │ cdb cleanup │──────────────────▶│CleanUpMotionLayerIPC│                                                                                             
	                            └─────────────┘                   └─────────────────────┘                                       
```


Notice:

- The ones starting with capital letters in the figure are methods in the interface.
- Notes starting with lowercase letters in the figure.

Interconnect contains three main data structures

- **ChunkTransportState**: Generated by `EState`, one-to-one correspondence with `EState` object.
   - Records most of the global information, such as remote connection information.
   - Contains a set of **ChunkTransportStateEntry**.
- **ChunkTransportStateEntry**: Generated by `SliceTable`, one-to-one correspondence with `motionId`.
   - Different interconnect implementations have different ** ChunkTransportStateEntry** objects, Obtain subclass objects through `CONTAINER_OF`.
	- In this structure, multiple links for managing a single motion node(Use `motionId` to identify). It is distinguished from incoming/outgoing
	- Contains a set of **MotionConn**
- **MotionConn**
	- Different interconnect implementations have different ** MotionConn** objects, Obtain subclass objects through `CONTAINER_OF`.
	- In this structure, a specific point-to-point connection is established.


# How to implements a interconnect type

Here I assume an optimization scenario: **in intercontect layer, support domain socket , used domain socket on local machine, and use udp implements when across machines**

Here are two pieces of pseudo-code to achieve this function

Solution1: call interfaces of `udpifc` in the `domain socket interface` which we will defined.

```
MotionIPCLayer domain_udp_ipc_layer = {
  .ic_type = INTERCONNECT_TYPE_DOMAIN_UDP, // new ic type
  .GetMaxTupleChunkSize = GetMaxTupleChunkSizeUDP, // udp header bigger than tcp, so use udp max size
  .GetListenPort = GetListenPortDomainUDP,  // need return a combined port. return type is `int32`, can hold both.


  /**
   * InitMotionIPCLayerDomainUDP() {
   *   // init domain socket ipc layer
   *   
   *   InitMotionIPCLayerUDP(); // also init udp ipc layer
   * }
   * 
   */ 
  .InitMotionLayerIPC = InitMotionIPCLayerDomainUDP, 

  /**
   * SetupInterconnectDomainUDP() {
   *   SetupInterconnectUDP(); // setup udp ipc layer before setup domain socket ipc layer
   *   // after SetupInterconnectUDP, `ChunkTransportState` have been inited
   *   // and some of `MotionConnUDP` have been init 
   *   
   *   SetupInterconnectDomainSocket(ChunkTransportState obj);
   *   // in SetupInterconnectDomainSocket should create some of object `MotionConnDomianUDP`
   *   // which contains `MotionConnUDP` and replace it in `ChunkTransportStateEntry->conns`
   * 
   *   // after this call, we can make sure that each `MotionConnDomianUDP` will use domain socket or udp?
   * }
   *
   */  
  .SetupInterconnect = SetupInterconnectDomainUDP,

  /**  
   * SendChunkDomainUDP() {
   *   // `MotionConnDomianUDP` use domain socket or udp send tuple
   * }
   */
  .SendTupleChunkToAMS = SendChunkDomainUDP,
  
  /**  
   * RecvTupleChunkDomainUDP() {
   *   // `MotionConnDomianUDP` use domain socket or udp recv tuple
   * }
   */  
  .RecvTupleChunk = RecvTupleChunkDomainUDP,


  ... other interfaces
}
```

Solution 2: Coupling the logic in `udpifc`, just like the current `proxy` implementation.

```
// still used udp interface 
// and add some struct inside `MotionConnUDP` + `ChunkTransportStateEntryUDP`
MotionIPCLayer udpifc_ipc_layer = {
    .ic_type = INTERCONNECT_TYPE_DOMAINUDP,
    .GetMaxTupleChunkSize = GetMaxTupleChunkSizeUDP,
    .GetListenPort = GetListenPortUDP,
    .InitMotionLayerIPC = InitMotionIPCLayerUDP,
    .SetupInterconnect = SetupInterconnectUDP,
    .SendChunk = SendChunkUDPIFC,
    .RecvTupleChunk = RecvTupleChunkUDPIFC,
}

// do some hook like ic_proxy implements
InitMotionIPCLayerUDP/SetupInterconnect() {

// origin logic 
#ifdef ENABLE_DOMAIN_SOCKET
// init/setup domain socket
#endif

}

SendChunk/RecvTupleChunk() {
// inited MotionConnUDP will make sure used domain socket or udp
#ifdef ENABLE_DOMAIN_SOCKET
   if (conn->ShouldUseDomainSocket()) {
      // send with domain socket 
      return;
   }
#endif 

 // origin udp logic 
}

```

# interconnect test && bench

In the path `contrib/interconnect/test`, the test of the interconnect and the benchmark are implemented, also need to be compiled separately.

For proxy type interconnect, user need to compile `cbdb` with `--enable-ic-proxy` to make the test take effect.

compile test and benchmark

```
cd contrib/interconnect/test
mkdir build && cd build
cmake ..
make -j
```

Notice that: for now, only `single client + single server` is supported in testing and benchmarking.

## bench result 

test env 

- system: CentOS Linux release 7.5.1804
- machine: qingcloud e2, x86, 8 cpu, 16G memory
- mtu: 1500
- buffer size: 200
- time: 100s


tcp result:

```
+----------------+------------+
| Total time(s)  |    100.000 |
| Loop times     |   48045111 |
| LPS(l/ms)      |    480.451 |
| Recv mbs       |      65430 |
| TPS(mb/s)      |    654.301 |
| Recv counts    |  336315777 |
| Items ops/ms   |   3363.157 |
+----------------+------------+
```

proxy result:

```
+----------------+------------+
| Total time(s)  |    100.118 |
| Loop times     |   11447670 |
| LPS(l/ms)      |    114.342 |
| Recv mbs       |      15589 |
| TPS(mb/s)      |    155.716 |
| Recv counts    |   80133690 |
| Items ops/ms   |    800.393 |
+----------------+------------+
```

udpifc result:

```
+----------------+------------+
| Total time(s)  |    100.079 |
| Loop times     |     369104 |
| LPS(l/ms)      |      3.688 |
| Recv mbs       |        502 |
| TPS(mb/s)      |      5.023 |
| Recv counts    |    2583728 |
| Items ops/ms   |     25.817 |
+----------------+------------+
```

Notice that: Lower TPS does not mean the protocol is slower, might means that the cpu time taken by the protocol is low. For the udpifc, it satisfies the highest tps required by `cbdb`. at the same time it occupies a lower cpu than other types of interconnect.

# interconnect statistics

This extension provides cumulative interconnect statistics for Apache Cloudberry, including queue sizes, buffer usage, retransmits, packet errors, and other UDPIFC‑related metrics. 

It exposes three views with statistics at different aggregation levels:
- gp_interconnect_stats — total cluster‑wide stats;
- gp_interconnect_stats_per_segment — stats per segment;
- gp_interconnect_stats_per_host — stats grouped by host.

## How to create the extension

Add interconnect to shared_preload_libraries and restart the cluster.

```
gpconfig -c shared_preload_libraries -v \
  "$(psql -At -c \
    "SELECT array_to_string( \
        array_append( \
          string_to_array( \
            current_setting('shared_preload_libraries'), \
            ','), \
          'interconnect'), \
        ',')" \
    postgres)"
gpstop -ra
```

Create the extension in your database.

```
CREATE EXTENSION interconnect;
```
