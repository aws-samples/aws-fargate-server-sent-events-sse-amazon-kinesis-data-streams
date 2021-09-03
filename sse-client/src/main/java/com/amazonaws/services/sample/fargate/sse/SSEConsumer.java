package com.amazonaws.services.sample.fargate.sse;

import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.internal.http2.StreamResetException;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSourceListener;
import okhttp3.sse.EventSources;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

import java.util.List;
import java.util.concurrent.TimeUnit;

/***
 * Handles connecting to the Server-Sent Events endpoint and reconnects if disconnected
 */
public class SSEConsumer implements Runnable {
    private volatile boolean running = true;
    private volatile boolean connected = false;
    private Logger logger;
    private String url = null;
    private String streamName = null;
    private Region region = null;
    private String[] headers;
    private List<String> collectTypes;
    private int readTimeoutMS = 0;
    private long messagesReceived = 0L;
    private long reportMessagesReceivedMS = 30000L;
    private boolean debug = false;

    public SSEConsumer setURL(String url) {
        this.url = url;
        return this;
    }

    public SSEConsumer setStreamName(String streamName) {
        this.streamName = streamName;
        return this;
    }

    public SSEConsumer setRegion(String region) {
        this.region = Region.of(region);
        return this;
    }

    public SSEConsumer setReadTimeoutMS(int readTimeoutMS) {
        this.readTimeoutMS = readTimeoutMS;
        return this;
    }

    public SSEConsumer setReportMessagesReceivedMS(long reportMessagesReceivedMS) {
        this.reportMessagesReceivedMS = reportMessagesReceivedMS;
        return this;
    }

    public SSEConsumer setCollectTypes(List<String> collectTypes) {
        this.collectTypes = collectTypes;
        return this;
    }

    public SSEConsumer setHeaders(String[] headers) {
        this.headers = headers;
        return this;
    }

    public SSEConsumer setLogger(Logger logger) {
        this.logger = logger;
        return this;
    }

    /***
     * This method handles the main Runnable method and will connect to the SSE endpoint and start receiving data.
     */
    @Override
    public void run() {
        if (!validateInput()) {
            return;
        }
        KinesisClient kinesisClient = KinesisClient.builder().region(region).build();

        // keep trying to reconnect if we get disconnected
        while (running) {
            messagesReceived = 0L; //reset the number of messages received since last connect
            final EventSource eventSource = createEventSource(kinesisClient);
            connected = true;
            reportWhileConnectedAndRunning();
            eventSource.cancel();
            logger.info("Stopping event source");
        }
    }

    private boolean validateInput() {
        logger.info("URL: " + (this.url == null || this.url.length() <= 0 ? "N/A" : this.url));
        logger.info("Kinesis Data Stream: " + (this.streamName == null || this.streamName.length() <= 0 ? "N/A" : this.streamName));
        logger.info("Region: " + (this.region == null ? "N/A" : this.region.toString()));
        logger.info("Headers: " + (this.headers == null || this.headers.length <= 0 ? "N/A" : String.join(",", this.headers)));
        logger.info("Collect Types: " + (this.collectTypes == null || this.collectTypes.size() <= 0 ? "N/A" : String.join(",", this.collectTypes)));
        logger.info("Read timeout: " + readTimeoutMS);
        logger.info("Report messages received MS: " + reportMessagesReceivedMS);

        if (this.url == null || this.url.length() <= 0) {
            logger.error("URL not specified");
            running = false;
            return false;
        }
        if (this.streamName == null || this.streamName.length() <= 0) {
            logger.error("Kinesis Data Stream not specified");
            running = false;
            return false;
        }
        if (this.region == null) {
            logger.error("Region not specified");
            running = false;
            return false;
        }
        return true;
    }

    private void reportWhileConnectedAndRunning() {
        try {
            long startTime = System.currentTimeMillis();
            // while we are connected and running we need to hold this thread and report messages received if that option is enabled.
            // SSE events are sent via a callback in another thread
            while (running && connected) {
                Thread.sleep(100);
                long endTime = System.currentTimeMillis();
                if (reportMessagesReceivedMS > 0 && (endTime - startTime > reportMessagesReceivedMS)) {
                    logger.info("SSE received [" + messagesReceived + "] events between [" + startTime + "] and [" + endTime + "]");
                    startTime = endTime;
                    messagesReceived = 0;
                }
            }
        } catch (InterruptedException e) {
            logger.error("Sleep timer interrupted", e);
        }
    }

    private EventSource createEventSource(final KinesisClient kinesisClient) {
        OkHttpClient client = new OkHttpClient.Builder()
                .readTimeout(readTimeoutMS, TimeUnit.MILLISECONDS)
                .retryOnConnectionFailure(true)
                .build();
        EventSourceListener listener = new EventSourceSender(kinesisClient, streamName, logger, collectTypes, debug);
        Request.Builder requestBuilder = new Request.Builder();

        // Allow the end user to specify url headers to send during the initial sse connection
        if (headers != null && headers.length > 0) {
            requestBuilder = requestBuilder.headers(Headers.of(headers));
        }

        // Create a request and connect using the standard headers for SSE endpoints
        Request request = requestBuilder
                .url(url)
                .header("Accept-Encoding", "")
                .header("Accept", "text/event-stream")
                .header("Cache-Control", "no-cache")
                .build();
        logger.info("Request created: " + request.toString());
        return EventSources.createFactory(client).newEventSource(request, listener);
    }

    /***
     * Set the flag to stop this application
     */
    public void shutdown() {
        running = false;
    }

    /***
     * This call will handle the actual SSE events and publish them to the Kinesis Data Streams stream via the collect call
     */
    public final class EventSourceSender extends EventSourceListener {
        private final Logger logger;
        private final List<String> collectTypes;
        private final KinesisClient kinesisClient;
        private final String streamName;
        private final boolean debug;
        private int publishErrorCount;

        public EventSourceSender(final KinesisClient kinesisClient, final String streamName, final Logger logger, final List<String> collectTypes, final boolean debug) {
            this.kinesisClient = kinesisClient;
            this.streamName = streamName;
            this.logger = logger;
            this.collectTypes = collectTypes;
            this.debug = debug;
            this.publishErrorCount = 0;
        }

        /***
         * Callback when the SSE endpoint connection is made. Currently all that is done is to log the event.
         * @param eventSource the event source
         * @param response the response
         */
        @Override public void onOpen(@NotNull EventSource eventSource, @NotNull Response response) {
            logger.info("Connected to SSE Endpoint");
        }

        /***
         * For each event received from the SSE endpoint we check if its a type requested and then publish to the Kinesis Data Streams stream via the collect call
         * @param eventSource The event source
         * @param id The id of the event
         * @param type The type of the event which is used to filter
         * @param data The event data
         */
        @Override public void onEvent(@NotNull EventSource eventSource, String id, String type, @NotNull String data) {
            if (collectTypes == null || collectTypes.contains(type)) {
                if (reportMessagesReceivedMS > 0) {
                    messagesReceived++;
                }
                publishRecord(data, type, this.streamName);
            }
        }

        /***
         * Publish an event to the Kinesis Data Streams stream using the event type as the partition key
         * @param data The actual data to publish to the stream
         * @param partition The partition which is the event type from the SSE
         * @param streamName The name of the stream to publish the event data into
         */
        public void publishRecord(@NotNull String data, final String partition, String streamName) {
            try {
                kinesisClient.putRecord(PutRecordRequest.builder()
                        .partitionKey(partition)
                        .streamName(streamName)
                        .data(SdkBytes.fromByteArray(data.getBytes()))
                        .build());
                publishErrorCount = 0;
                logger.debug("Published on '" + streamName + "' partition '" + partition + "' data: " + data);
            } catch (KinesisException e) {
                // reduce log blow out of security failures and other repeated errors on publish
                if (publishErrorCount < 2) {
                    logger.error("Error publishing on '" + streamName + "' partition '" + partition + "' data: " + data, e);
                    publishErrorCount++;
                }
            }
        }

        /***
         * When the connection is closed we receive this even which is currently only logged.
         * @param eventSource The event source
         */
        @Override public void onClosed(@NotNull EventSource eventSource) {
            logger.info("Closed");
        }

        /***
         * If there is any failure we log the error and the stack trace
         * During stream resets with no errors we set the connected flag to false to allow the main thread to attempt a re-connect
         * @param eventSource The event source
         * @param t The error object
         * @param response The response
         */
        @Override
        public void onFailure(@NotNull EventSource eventSource, Throwable t, Response response) {
            if (t != null) {
                logger.error("Error: " + t.getMessage(), t);
                if (t instanceof StreamResetException && t.getMessage().contains("NO_ERROR")) {
                    connected = false;
                }
            }
        }
    }
}
