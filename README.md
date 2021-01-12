# kdb-chronicle-queue
Java adaptor from Chronicle queue to kdb+

Repo contains: 
- Producer app that writes messages to a Chronicle Queue. 
- A sample kdb+ database. 
- An Adapter app that reads messages from Chronicle Queue and writes them to kdb+ 

## Producer

### Arguments (Properties file)

--spring.config.location=classpath:/,<PATH_TO_FILE>

#### Properties file contains:

- server.port: What port to run web component on e.g. 9090
- server.servlet.context-path: Path to access web component on e.g. /producer
- producer.messageFrequency: How freqently (in millis) to send a message to the queue e.g. 1000 for every second
- chronicle.quote.queue: Destination filesystem folder for queue to be persisted e.g. C:\\ChronicleQueue\\Producer\\quote

#### Example:

> server.port=9090 <br />
> server.servlet.context-path=/producer <br />
> producer.messageFrequency=10 <br />
> chronicle.quote.queue=C:\\ChronicleQueue\\Producer\\quote <br />

### Running Producer
Run java application as follows:

- java -jar producer-0.1.jar --spring.config.location=classpath:/,C:/gitWorkarea/kdb-chronicle-queue/Producer/config/producer.properties
- Start message creation as follows:
-- Http GET request to [http://localhost:9090/producer/quoteLoader?Command%3A%20start%2Fstop=start]
- Stop message creation as follows:
-- Http GET request to [http://localhost:9090/producer/quoteLoader?Command%3A%20start%2Fstop=stop]

## Adapter
### Arguments (Properties file)

--spring.config.location=classpath:/,<PATH_TO_FILE>

#### Properties file contains:

- server.port: What port to run web component on e.g. 8080
- chronicle.source: Patch to Chronicle Queue file
- adapter.tailerName: Name Queue tailer to enable stop / re-start
- kdb.host: Server running kdb+ database
- kdb.port: kdb+ port
- kdb.login: kdb+ login credentials
- kdb.connection-enabled=true
- kdb.destination: Name of kdb+ table to write to
- kdb.destination.function: Name of function to use when writing to table

#### Example:

> server.port=8080 <br />
> management.endpoint.shutdown.enabled=true <br />
> management.endpoint.info.enabled=true <br />
> management.endpoints.web.exposure.include=* <br />
> management.endpoints.web.base-path=/adapter <br />
> management.endpoint.health.show-details=always <br />
> chronicle.source=C:\\ChronicleQueue\\Producer\\quote <br />
> adapter.tailerName=quoteTailer <br />
> kdb.host=localhost <br />
> kdb.port=5000 <br />
> kdb.login=username:password <br />
> kdb.connection-enabled=true <br />
> kdb.destination=quote <br />
> kdb.destination.function=.u.upd <br />

### Starting
Run java application as follows:
- java -jar adapter-0.1.jar --spring.config.location=classpath:/,C:/gitWorkarea/kdb-chronicle-queue/Adapter/config/adapter.properties

If app can connect to source queue and kdb+ destination it will start transfer immendiately.

## Use of Spring Boot Actuator in Adapter

Spring Boot Actuator allows interrogation, monitoring and interaction with configured Spring Boot applications.

### Configured to date...
```sh
- Adapter App Health: http://localhost:8080/adapter/health
- Adapter processing status: http://localhost:8080/adapter/status
- Shutdown Adapter: http://localhost:8080/adapter/shutdown
```
