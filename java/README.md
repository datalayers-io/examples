# Java Examples

用 Arrow Flight SQL 客户端（`org.apache.arrow.flight.sql.FlightSqlClient`）连接 Datalayers 的示例。

## 依赖

- JDK 8+（el7：`yum install -y java-1.8.0-openjdk-devel`；Ubuntu：`apt-get install -y openjdk-11-jdk-headless`）
- Maven 3.6+（el7 无官方包，下载二进制：`curl -fsSL -o /tmp/mvn.tgz https://archive.apache.org/dist/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz && tar -xzf /tmp/mvn.tgz -C /opt/maven --strip-components=1`，国内可配 aliyun 镜像 `~/.m2/settings.xml` 的 `<mirror>`）

## 编译

```bash
cd java
mvn package
```

## 运行

```bash
# 拷贝依赖到 target/libs（jar 的 manifest 依赖 classpathPrefix=libs/）
mvn dependency:copy-dependencies -DoutputDirectory=target/libs
java -jar target/ArrowFilghtSqlTest-1.0-SNAPSHOT.jar
```

> 示例默认连接 `127.0.0.1:8360`（Arrow Flight SQL 端口），账号 `admin`/`public`，库 `test`。
> 若 Datalayers 在其他地址/端口，修改 `SqlRunner.java` 中 `Location.forGrpcInsecure(...)` 的地址与端口后重新 `mvn package`。
> 例：POC 测试集群使用偏移端口 18360 时，改为 `Location.forGrpcInsecure("127.0.0.1", 18360)`。
