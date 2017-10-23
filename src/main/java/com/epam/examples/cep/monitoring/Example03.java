package com.epam.examples.cep.monitoring;

import com.epam.examples.cep.monitoring.events.*;
import com.epam.examples.cep.monitoring.sources.MonitoringEventSource;
import com.epam.examples.cep.monitoring.sources.SensorEventSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.contrib.siddhi.SiddhiCEP;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class Example03 {
    private static final long PAUSE = 100;

    private static final double TEMPERATURE_THRESHOLD = 100;
    private static final double PRESSURE_THRESHOLD = 124_544;

    private static final int MAX_RACK_ID = 10;
    private static final int MAX_SENSOR_ID = 10;

    private static final double TEMPERATURE_RATIO = 0.5;
    private static final double POWER_STD = 10;
    private static final double POWER_MEAN = 100;
    private static final double TEMP_STD = 25;
    private static final double TEMP_MEAN = 80;
    private static final double PRESSURE_STD = 29_420;
    private static final double PRESSURE_MEAN = 98_066.5;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Use ingestion time => TimeCharacteristic == EventTime + IngestionTimeExtractor
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().disableSysoutLogging();

        // Create Siddhi CEP environment
        SiddhiCEP cep = SiddhiCEP.getExtendedSiddhiEnvironment(env);

        // Input stream of monitoring events
        DataStream<MonitoringEvent> inputMonitoringEventStream = env
                .addSource(new MonitoringEventSource(
                        MAX_RACK_ID,
                        PAUSE,
                        TEMPERATURE_RATIO,
                        POWER_STD,
                        POWER_MEAN,
                        TEMP_STD,
                        TEMP_MEAN))
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        // Input stream of sensor events
        DataStream<SensorEvent> inputSensorEventStream = env
                .addSource(new SensorEventSource(
                        MAX_SENSOR_ID,
                        PAUSE,
                        PRESSURE_STD,
                        PRESSURE_MEAN))
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());


        // ***** TEMPERATURE EVENTS *****

        DataStream<Tuple2<Integer, Double>> tempEvents = inputMonitoringEventStream
                .filter((MonitoringEvent event) -> event.getClass().equals(TemperatureEvent.class))
                .map((MonitoringEvent event) -> (TemperatureEvent) event)
                .map((TemperatureEvent event) -> new Tuple2<>(event.getRackID(), event.getTemperature()))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Double.class));


        // ***** PRESSURE EVENTS *****

        DataStream<Tuple2<Integer, Double>> presEvents = inputSensorEventStream
                .filter((SensorEvent event) -> event.getClass().equals(PressureEvent.class))
                .map((SensorEvent event) -> (PressureEvent) event)
                .map((PressureEvent event) -> new Tuple2<>(event.getSensorID(), event.getPressure()))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Double.class));


        // ***** PANIC ALERTS *****

        cep.registerStream("tempEvents", tempEvents, "rackID", "temperature");
        cep.registerStream("presEvents", presEvents, "sensorID", "pressure");

        String query = readResource("query.cql")
                .replace("${TEMPERATURE_THRESHOLD}", "" + TEMPERATURE_THRESHOLD)
                .replace("${PRESSURE_THRESHOLD}", "" + PRESSURE_THRESHOLD);

        DataStream<Tuple2<Integer, Integer>> cepOutput = cep.from("tempEvents").union("presEvents")
                .cql(query)
                .returns("output");

        DataStream<PanicAlert> panicAlerts = cepOutput
                .map((Tuple2<Integer, Integer> value) -> new PanicAlert(value.f0, value.f1))
                .returns(PanicAlert.class);

        panicAlerts.print();

        env.execute("CEP monitoring job");
    }

    private static String readResource(String name) {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        try (InputStream inputStream = classloader.getResourceAsStream(name);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))
        ) {
            return reader.lines().collect(Collectors.joining("\n"));
        } catch (Exception e) {
            return null;
        }
    }
}
