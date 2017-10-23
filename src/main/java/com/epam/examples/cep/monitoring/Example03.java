package com.epam.examples.cep.monitoring;

import com.epam.examples.cep.monitoring.events.MonitoringEvent;
import com.epam.examples.cep.monitoring.events.SensorEvent;
import com.epam.examples.cep.monitoring.events.TemperatureEvent;
import com.epam.examples.cep.monitoring.sources.MonitoringEventSource;
import com.epam.examples.cep.monitoring.sources.SensorEventSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.contrib.siddhi.SiddhiCEP;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;

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

        // Create Siddhi CEP environment
        SiddhiCEP cep = SiddhiCEP.getSiddhiEnvironment(env);

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

        DataStream<Tuple2<Integer, Double>> tempWarnings = cep.from("tempEvents", tempEvents, "rackID", "temperature")
                .cql("from every e1 = tempEvents[temperature > " + TEMPERATURE_THRESHOLD + "] -> " +
                        "e2 = tempEvents[temperature > " + TEMPERATURE_THRESHOLD + " and rackID == e1.rackID] " +
                        "within 10 seconds " +
                        "select e1.rackID, (e1.temperature + e2.temperature) / 2 as temperature " +
                        "insert into tempWarnings")
                .returns("tempWarnings");

        DataStream<Tuple1<Integer>> tempAlerts = cep.from("tempWarnings", tempWarnings, "rackID", "temperature")
                .cql("from every e1 = tempWarnings -> " +
                        "e2 = tempWarnings[rackID == e1.rackID and temperature > e1.temperature] " +
                        "within 20 seconds " +
                        "select e1.rackID insert into output")
                .returns("output");

        tempAlerts.print();

        // ***** PRESSURE EVENTS *****


        // ***** PANIC ALERTS *****

        env.execute("CEP monitoring job");
    }
}
