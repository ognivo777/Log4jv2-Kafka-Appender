# Log4jv2-Kafka-Appender
## About
An kafka appender adopted to use with Pega PRPC system.

Based on original log4j-kafka appender.

### Changes

* Isolate kafka client classloader
* Changed default kafka batch.size
* Added default JSONLayout

## Usage

1. Put latest **log4j2-kafka-appender.jar** from releases page into your classpath.
1. Add new appender into log4j2.xml:
```xml
<Configuration status="warn">
    <Appenders>
    <!--...-->
    <ObizKafka name="Kafka" topic="logstash-input-topic">
        <Property name="bootstrap.servers">kafka-host:9092</Property>
    </ObizKafka>
    <!--...-->
    </Appenders>
<!--...-->
</Configuration>
```
1. Add them into root:
```xml
<Configuration status="warn">
<!--...-->
	<Loggers>
		<asyncRoot>
			<AppenderRef ref="CONSOLE"/>
			<AppenderRef ref="Kafka"/>
		</asyncRoot>
	</Loggers>
	<!--...-->
</Configuration>	
```

## Logstash

Example of Logstash input
```yaml
input {
  kafka {
    bootstrap_servers => "kafka-host:9092"
    topics => ["logstash-input-topic"]
    codec => json
  }	
}
```
