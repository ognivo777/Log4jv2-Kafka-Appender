package org.obizsoft.log4j2.appender;

import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.mom.kafka.KafkaManager;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.*;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Plugin(
        name = "ObizKafka",
        category = Core.CATEGORY_NAME,
        elementType = Appender.ELEMENT_TYPE)
public class ObizKafka extends AbstractAppender {

    private final KafkaManager manager;

    private ObizKafka(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions, final KafkaManager manager) {
        super(name, filter, layout, ignoreExceptions);
        this.manager = Objects.requireNonNull(manager, "manager");
        AbstractLifeCycle.LOGGER.error("TST: ObizKafka created!");
    }

    @PluginFactory
    public static ObizKafka createAppender(
        @PluginAttribute("name") String name,
        @PluginElement("Layout") Layout<? extends Serializable> layout,
        @PluginElement("Filter") Filter filter,
        @PluginElement("Property") Property[] propertyArray,
        @PluginAttribute("topic") String topic,
        @PluginAttribute(value = "syncSend") boolean syncSend,
        @PluginAttribute(value = "ignoreExceptions") boolean ignoreExceptions) {
        if (name == null) {
            LOGGER.error("No name provided for MyCustomAppenderImpl");
            return null;
        }
        if (layout == null) {
            AbstractLifeCycle.LOGGER.error("No layout provided for ObizKafka. Use default.");
            try {
                layout = JsonLayout.newBuilder()
                        .setComplete(false)
                        .setCompact(true)
                        .setEventEol(true)
                        .setProperties(true)
                        .build();
            } catch (Exception e) {
                //may be there is no needed classes in classpath, so create ugly PatternLayout than
                layout = PatternLayout.newBuilder().withPattern("%d{DEFAULT_MICROS} [%t]<%c{2.}> %-5p: %m%n").build();
            }
        }

        boolean hasBatchSize = false;
        for (int i = 0; i < propertyArray.length; i++) {
            Property property = propertyArray[i];
            if(property.getName().equals("batch.size")){
                hasBatchSize = true;
                break;
            }
        }

        if(!hasBatchSize) {
            propertyArray = Arrays.copyOf(propertyArray, propertyArray.length + 1);
            propertyArray[propertyArray.length - 1] =  Property.createProperty("batch.size", "16384"); //as default of Kafka client
        }

        AbstractLifeCycle.LOGGER.error("TST: Before create KafkaManager");
        final KafkaManager kafkaManager = new KafkaManager(LoggerContext.getContext(), name, topic, syncSend, propertyArray);
        AbstractLifeCycle.LOGGER.error("TST: After create KafkaManager");
        return new ObizKafka(name, filter, layout, ignoreExceptions, kafkaManager);

    }

    public void append(LogEvent event) {
        if (event.getLoggerName() != null && event.getLoggerName().startsWith("org.apache.kafka")) {
            LOGGER.warn("Recursive logging from [{}] for appender [{}].", event.getLoggerName(), getName());
        } else {
            try {
                tryAppend(event);
            } catch (final Exception e) {
                error("Unable to write to Kafka in appender [" + getName() + "]", event, e);
            }
        }

    }

    private void tryAppend(final LogEvent event) throws ExecutionException, InterruptedException, TimeoutException {
        final Layout<? extends Serializable> layout = getLayout();
        byte[] data;
        data = layout.toByteArray(event);
        manager.send(data);
    }

    @Override
    public void start() {
        super.start();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);
        AbstractLifeCycle.LOGGER.error("TST: Current thread classloader is set to null.");
        try{
            AbstractLifeCycle.LOGGER.error("TST: Before start manager!");
            manager.startup();
            AbstractLifeCycle.LOGGER.error("TST: After start manager!");
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
            AbstractLifeCycle.LOGGER.error("TST: Current thread classloader restored.");
        }
    }

    @Override
    public boolean stop(final long timeout, final TimeUnit timeUnit) {
        setStopping();
        boolean stopped = super.stop(timeout, timeUnit, false);
        stopped &= manager.stop(timeout, timeUnit);
        setStopped();
        return stopped;
    }

    @Override
    public String toString() {
        return "ObizKafka{" + "name=" + getName() + ", state=" + getState() + ", topic=" + manager.getTopic() + '}';
    }


}
