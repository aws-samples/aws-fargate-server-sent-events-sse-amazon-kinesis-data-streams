package com.amazonaws.services.sample.fargate.sse;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/***
 * Main class to handle the command line options and creating the main runnable instance
 */
public class ServerSentEvents {
    public static final String URL = "url";
    public static final String STREAM = "stream";
    public static final String REGION = "region";
    public static final String REPORTMS = "reportms";
    public static final String READTIMEOUTMS = "readtimeoutms";
    public static final String HEADERS = "headers";
    public static final String TYPES = "types";
    private static Thread consumerThread;
    private static SSEConsumer consumer;

    public static void main(String [] args) {
        Logger logger = LoggerFactory.getLogger(SSEConsumer.class);
        Options options = new Options();
        Option url = new Option("u", URL, true, "SSE Endpoint URL");
        url.setRequired(true);
        options.addOption(url);

        Option streamName = new Option("s", STREAM, true, "Kinesis Data Streams stream name");
        streamName.setRequired(true);
        options.addOption(streamName);

        Option region = new Option("r", REGION, true, "Kinesis Data Streams Region");
        region.setRequired(true);
        options.addOption(region);

        Option headers = new Option("h", HEADERS, true, "Headers to send to the SSE endpoint in the form of header1,value1,header2,value2");
        options.addOption(headers);

        Option types = new Option("t", TYPES, true, "A comma seperated list of types of SSE events to publish to the Kinesis Data Streams stream");
        options.addOption(types);

        options.addOption(new Option("rms", REPORTMS, true, "Log how many messages have been received every X milliseconds, 0 [default] to disable"));
        options.addOption(new Option("rt", READTIMEOUTMS, true, "The read timeout value in milliseconds to use. It is highly recommended to keep this to the default of 0. The system may not function with a timeout set."));

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        try {
            CommandLine cmd = parser.parse(options, args);

            consumer = new SSEConsumer()
                    .setLogger(logger)
                    .setURL(cmd.getOptionValue(URL))
                    .setRegion(cmd.getOptionValue(REGION))
                    .setStreamName(cmd.getOptionValue(STREAM));

            String headerValues = cmd.getOptionValue(HEADERS);
            if (headerValues != null && headerValues.length() > 0) {
                consumer.setHeaders(headerValues.split(","));
            }
            String typeValues = cmd.getOptionValue(TYPES);
            if (typeValues != null && typeValues.length() > 0) {
                consumer.setCollectTypes(Arrays.asList(typeValues.split(",")));
            }
            consumer.setReportMessagesReceivedMS(Integer.parseInt(cmd.getOptionValue(REPORTMS, "30000")));
            consumer.setReadTimeoutMS(Integer.parseInt(cmd.getOptionValue(READTIMEOUTMS, "0")));
            consumerThread = new Thread(consumer);

            // Add a shutdown hook to monitor for control breaks, then we can gracefully shut down the SSE consumer
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                consumer.shutdown();
                try {
                    consumerThread.join(1000);
                } catch (InterruptedException e) {
                    logger.error("Consumer was slow to shutdown.", e);
                }
            }));
            consumerThread.start();
            consumerThread.join();
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("ServerSentEvents", options);
            System.exit(1);
        } catch (InterruptedException e) {
            logger.error("Error: " + e.getMessage(), e);
        }
    }
}
