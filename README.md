# Kiso: A Rust Service Foundation

This repository contains the code for the `kiso` runtime, based on tokio (a WIP version
based on glommio is available in the `glommio` branch). The runtime is built with the
purpose of serving as a production-ready foundation for HTTP/gRPC network services.

Kiso provide features for all layers of the service:

* Composable and easy to use configuration system based on command line arguments via `clap`.
* Configurable and performance-focused tokio runtime focused in I/O bound services. 
* Automatic stall-detection for tasks blocking the underlying executor.
* Properly configured hyper server, serving tonic services and axum routers.
* Performant observability suite, built on top of the OpenTelemetry crates.
