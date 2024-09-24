# Datalayers Examples

Examples and code snippets demonstrating common ways of interacting Datalayers with various languages through the Arrow FlightSQL protocol.

## Language Support

The following table demonstrates what languages we support and what features we have implemented for this language.

| | connect | connect + TLS | insert | select | insert + prepared | select + prepared |
|:-------:|:---------:|:----------------:|:--------:|:--------:|:-------------------:|:-------------------:|
| Go     | ✔       | ✔              | ✔      | ✔      | ✔                 | ✔                 |
| Python | ✔       | ✔              | ✔      | ✔      | ✔                 | ✔                 |
| Rust   | ✔       | ✔              | ✔      | ✔      | ✔                 | ✔                 |
| Java   | ✔       | ✖              | ✔      | ✔      | ✖                 | ✖                 |

The details of each feature are outlined below:

- connect: insecure connection with basic authentication.
- connect + TLS: secure connection with basic authentication + TLS.
- insert: insertion with plain INSERT statements.
- select: query with plain SELECT statements.
- insert + prepared: insertion with prepared INSERT statements.
- select + prepared: query with prepared SELECT statements.

## Run Examples

We have provided a Makefile to facilitate running examples for the supported languages.

Before running the examples, please ensure that all dependencies are installed. Each language has its own dedicated README file located in its respective subdirectory. These README files contain detailed instructions for installing dependencies and running the examples.
Alternatively, you can also simply execute `make build` for downloading, installing and building for all languages.

Note, these examples and commands were tested solely on Ubuntu 22.04 and may require tuning for other operating systems.

Run Go examples:

``` shell
make go
```

Run Python examples:

``` shell
make python
```

Run Rust examples:

``` shell
make rust
```

Run Java examples:

``` shell
make java
```
