# Go Examples

## Install dependencies

Install Go:

``` shell
wget -q https://go.dev/dl/go1.22.3.linux-amd64.tar.gz
rm -rf /usr/local/go && tar -C /usr/local -xzf go1.22.3.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```

Install dependencies:

``` shell
go mod tidy
```

## Run

``` shell
go build && ./main
```
