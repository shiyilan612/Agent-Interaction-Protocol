## Introduction

### Conception

<p align="center"><img src="https://github.com/user-attachments/assets/b43e74cf-cb86-4d43-91c7-4409d84aa7c8" width = "600" height = "300"></p>

The ​​Agent Interaction Protocol (AIP)​​ is a distributed agent interaction protocol designed for multi-agent collaboration. AIP supports both agent-to-agent connectivity and agent-to-tool invocation. In addition to point-to-point connections, all agents and tools can be mounted as nodes on a shared gateway to form an interconnected network.

### Characteristics

- AIP supports both centralized proxy and peer-to-peer communication.
- AIP uses ​​gRPC​​ as the underlying communication protocol, enabling faster serialization/deserialization through binary data transmission.
- Agents communicate via ​​bidirectional asynchronous streams​​, supporting full-duplex real-time interaction.

### Architecture

<p align="center"><img src="https://github.com/user-attachments/assets/7e593a88-ab0a-42fc-bca1-fd04fe739755" width = "300" height = "240"></p>

AIP adopts a layered design:

- ​​Module Layer​​: Implements business logic distribution and coordination in the form of nodes or gateways.
​​- Session Layer​​: Manages the lifecycle of each communication session between nodes, using unique session IDs and asynchronous task coordination to ensure real-time message processing and resource safety.
- ​​Transport Layer​​: Handles message dispatching and reception between the Session Layer and gRPC interfaces.

<p align="center"><img src="https://github.com/user-attachments/assets/e674e3c6-b13c-43d2-a8c5-efb306240bd7" width = "600" height = "400"></p>

AIP supports two communication modes:

​​- Request-Response​​: Used for agent-to-tool interactions.
- ​​Bidirectional Asynchronous Streaming​​: Used for real-time full-duplex agent-to-agent communication. The session initiator is responsible for sending session termination requests.