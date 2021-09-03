package com.amazonaws.services.sample.fargate.sse;

import software.amazon.awscdk.core.*;
import software.amazon.awscdk.services.ec2.*;
import software.amazon.awscdk.services.ecr.assets.DockerImageAsset;
import software.amazon.awscdk.services.ecs.*;
import software.amazon.awscdk.services.iam.*;
import software.amazon.awscdk.services.kinesis.Stream;
import software.amazon.awscdk.services.kinesis.StreamEncryption;
import software.amazon.awscdk.services.kinesisfirehose.DeliveryStream;
import software.amazon.awscdk.services.kinesisfirehose.destinations.S3Bucket;
import software.amazon.awscdk.services.logs.RetentionDays;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/***
 * Create the ECS Fargate stack for starting up ECS Fargate containers that connect to SSE and stream events to Kinesis
 */
public class ECSFargateServerSentEventsStack extends Stack {
    private Vpc vpc;
    private SecurityGroup securityGroup;
    private Role fargateTaskRole;
    private Stream outputDataStream;
    private IBucket s3StorageBucket;
    private DockerImageAsset dockerImageAsset;
    private CfnParameter urlParam;
    private CfnParameter headersParam;
    private CfnParameter collectTypesParam;
    private CfnParameter readTimeoutParam;
    private CfnParameter reportMSParam;
    private CfnParameter cpuCapacityParam;
    private CfnParameter memoryLimitParam;
    private CfnParameter s3StorageBucketParam;
    private CfnParameter s3StoragePrefixParam;
    private CfnParameter s3StorageErrorPrefixParam;

    public ECSFargateServerSentEventsStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public ECSFargateServerSentEventsStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);
        createParameters();
        lookupBuckets();
        createVPC();
        createKinesisDataStream();
        createKinesisFirehose();
        createFargateTaskRole();
        createDockerImage();
        createCluster();
    }

    /***
     * Create all the parameters required to allow the end user to modify the stack
     */
    private void createParameters() {
        urlParam = CfnParameter.Builder.create(this, "URL")
                .type("String")
                .description("The Server-Sent Events (SSE) endpoint")
                .allowedPattern(".+")
                .build();
        headersParam = CfnParameter.Builder.create(this, "Headers")
                .type("String")
                .description("Headers to send to the SSE endpoint in the form of header1,value1,header2,value2")
                .defaultValue("")
                .build();
        collectTypesParam = CfnParameter.Builder.create(this, "CollectTypes")
                .type("String")
                .description("A comma seperated list of types of SSE events to publish to the Kinesis Data Streams stream")
                .defaultValue("")
                .build();
        readTimeoutParam = CfnParameter.Builder.create(this, "ReadTimeout")
                .type("Number")
                .description("The read timeout value in milliseconds to use. It is highly recommended to keep this to the default of 0. The system may not function with a timeout set.")
                .defaultValue("0")
                .build();
        reportMSParam = CfnParameter.Builder.create(this, "ReportMS")
                .type("Number")
                .description("Log how many messages have been received every X milliseconds, 0 [default] to disable")
                .defaultValue("30000")
                .build();
        cpuCapacityParam = CfnParameter.Builder.create(this, "CPUCapacity")
                .type("Number")
                .description("The CPU capacity 256 (.25 vCPU), 512 (.5 vCPU), 1024 (1 vCPU), 2048 (2 vCPU), 4096 (4 vCPU)")
                .defaultValue("512")
                .build();
        memoryLimitParam = CfnParameter.Builder.create(this, "MemoryLimit")
                .type("Number")
                .description("The amount (in MiB) of memory used by the task. 512 (0.5 GB), 1024 (1 GB), 2048 (2 GB), Between 1024 (1 GB) and 30720 (30 GB) in increments of 1024 (1 GB)")
                .defaultValue("1024")
                .build();
        s3StorageBucketParam = CfnParameter.Builder.create(this, "S3StorageBucket")
                .type("String")
                .description("The S3 bucket name used to store the server-sent events data")
                .allowedPattern(".+")
                .build();
        s3StoragePrefixParam = CfnParameter.Builder.create(this, "S3StorageBucketPrefix")
                .type("String")
                .description("The prefix used when storing server-sent events data into the S3 bucket")
                .defaultValue("sse-data")
                .build();
        s3StorageErrorPrefixParam = CfnParameter.Builder.create(this, "S3StorageBucketErrorPrefix")
                .type("String")
                .description("The prefix used when storing error events into the S3 bucket")
                .defaultValue("sse-error")
                .build();
    }

    /***
     * Look up the bucket and make sure it exists before proceeding
     */
    private void lookupBuckets() {
        s3StorageBucket = Bucket.fromBucketName(this, "S3StorageBucketCheck", s3StorageBucketParam.getValueAsString());
    }

    /***
     * Create a VPC for the instances to run in
     */
    private void createVPC() {
        vpc = Vpc.Builder.create(this, "FargateSSEVPC")
                .cidr("10.0.0.0/16")
                .natGateways(1)
                .maxAzs(3)
                .subnetConfiguration(List.of(
                        SubnetConfiguration.builder().cidrMask(24).subnetType(SubnetType.PRIVATE).name("FargateSSE-Private").build(),
                        SubnetConfiguration.builder().cidrMask(24).subnetType(SubnetType.PUBLIC).name("FargateSSE-Public").build()))
                .build();
        securityGroup = SecurityGroup.Builder.create(this, "FargateSSESecurityGroup")
                .vpc(vpc)
                .allowAllOutbound(true)
                .description("Security group for Fargate Server-Sent Events")
                .build();
    }

    /***
     * This creates the Kinesis data streams stream which we publish the SSE events into
     */
    private void createKinesisDataStream() {
        outputDataStream = Stream.Builder.create(this, "FargateSSEKinesisDataStream")
                .shardCount(1)
                .retentionPeriod(Duration.hours(24))
                .encryption(StreamEncryption.UNENCRYPTED)
                .build();
    }

    /***
     * This creates the Kinesis Data Firehose which receives data from the data stream and pushes them to an S3 bucket
     */
    private void createKinesisFirehose() {
        final S3Bucket s3DestinationBucket = S3Bucket.Builder.create(s3StorageBucket)
                .bufferingInterval(Duration.seconds(60))
                .dataOutputPrefix(s3StoragePrefixParam.getValueAsString())
                .errorOutputPrefix(s3StorageErrorPrefixParam.getValueAsString())
                .bufferingSize(Size.mebibytes(5))
                .build();
        DeliveryStream.Builder.create(this, "FargateSSEKinesisFirehoseS3Delivery")
                .sourceStream(outputDataStream)
                .encryption(software.amazon.awscdk.services.kinesisfirehose.StreamEncryption.UNENCRYPTED)
                .destinations(List.of(
                        s3DestinationBucket
                ))
                .build();
    }

    /***
     * Create a role for the ECS Fargate instances to be able to connect to the Kinesis Data Stream
     */
    private void createFargateTaskRole() {
        fargateTaskRole = Role.Builder.create(this, "FargateSSETaskRole")
                .assumedBy(ServicePrincipal.Builder.create("ecs-tasks.amazonaws.com").build())
                .build();
        outputDataStream.grantReadWrite(fargateTaskRole);
    }

    /***
     * Create a docker image from the docker file which pulls the jar that has code to connect to SSE endpoints and send events to Kinesis
     */
    private void createDockerImage() {
        dockerImageAsset = DockerImageAsset.Builder.create(this, "FargateSSEDockerImageBuilder")
                .directory("docker")
                .build();
    }

    /***
     * Create the actual ECS clusters
     */
    private void createCluster() {
        createFargateService(
                Cluster.Builder.create(this, "FargateSSECluster")
                    .enableFargateCapacityProviders(true)
                    .vpc(vpc)
                    .build(),
                createTaskDefinition());
    }

    /***
     * Creates the task definition
     * @return The task definition
     */
    private FargateTaskDefinition createTaskDefinition() {
        FargateTaskDefinition  fargateTaskDefinition = FargateTaskDefinition.Builder.create(this, "FargateSSETaskDef")
                .cpu(cpuCapacityParam.getValueAsNumber())
                .memoryLimitMiB(memoryLimitParam.getValueAsNumber())
                .taskRole(fargateTaskRole)
                .build();
        fargateTaskDefinition.addContainer("FargateSSE",
                ContainerDefinitionOptions.builder()
                        .logging(LogDriver.awsLogs(AwsLogDriverProps.builder()
                                .logRetention(RetentionDays.FIVE_DAYS)
                                .streamPrefix("FargateSSE")
                                .build()))
                        .environment(createEnvironmentVariableMap())
                        .image(ContainerImage.fromDockerImageAsset(dockerImageAsset))
                        .build()
        );
        return fargateTaskDefinition;
    }

    /***
     * Setup the environment variables used with the docker image
     * @return The environment variables
     */
    private Map<String, String> createEnvironmentVariableMap() {
        Map<String, String> env = new HashMap<>();
        env.put("stream", "\"" + outputDataStream.getStreamName() + "\"");
        env.put("region", "\"" + getRegion() + "\"");
        env.put("url", "\"" + urlParam.getValueAsString() + "\"");
        env.put("reportMS", reportMSParam.getValueAsString());
        env.put("readtimeoutMS", readTimeoutParam.getValueAsString());
        env.put("headers", "\"" + headersParam.getValueAsString() + "\"");
        env.put("collectTypes", "\"" + collectTypesParam.getValueAsString() + "\"");
        return env;
    }

    /***
     * Create a Fargate service with Spot instances weighted higher
     * @param cluster The cluster used when creating instances
     * @param fargateTaskDefinition The Fargate task definition
     */
    private void createFargateService(Cluster cluster, FargateTaskDefinition  fargateTaskDefinition) {
        FargateService.Builder.create(this, "FargateSSEService")
                .cluster(cluster)
                .taskDefinition(fargateTaskDefinition)
                .capacityProviderStrategies(List.of(
                        CapacityProviderStrategy.builder()
                                .capacityProvider("FARGATE_SPOT")
                                .weight(2)
                                .build(),
                        CapacityProviderStrategy.builder()
                                .capacityProvider("FARGATE")
                                .weight(1)
                                .build()))
                .build();
    }
}
