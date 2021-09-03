package com.amazonaws.services.sample.fargate.sse;

import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.StackProps;

public class ECSFargateServerSentEventsApp {
    public static void main(final String[] args) {
        App app = new App();
        new ECSFargateServerSentEventsStack(app, "ECSFargateSSE", StackProps.builder().build());
        app.synth();
    }
}
