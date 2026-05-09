# Simple kafka order service
## Architecture

```mermaid
graph TD
    %% Define the User
    User((User/Client))
    Kafka((Kafka topic: order-placed))
    
    %% Define the Controller logic
    Controller[Order Controller]
    CheckToken{Token Present?}
    
    %% Define the Outcomes
    Error429[/429 Too Many Requests/]
    Process[Proceed to Service Logic]

    %% Connections
    User -->|POST /orders| Controller
    Controller --> CheckToken

    CheckToken -- No --> Error429
    CheckToken -- Yes --> Process

    %% Return path for error
    Error429 -.->|Response| User
    Process --> |Send to kafka topic| Kafka
```

### Core
The service will expose a rest endpoint, which can be used to place an order,
which further sends a message to order-place topic.

### Rate limiting
This service will use token bucket algorithm to ensure rate limiting to prevent
system overload. The tokens will be refreshed from time to time.
