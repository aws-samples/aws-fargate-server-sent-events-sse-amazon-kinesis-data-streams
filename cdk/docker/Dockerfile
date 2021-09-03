FROM amazoncorretto:16
ADD aws-fargate-server-sent-events-sse-amazon-kinesis-data-streams-1.0.0.jar aws-fargate-server-sent-events-sse-amazon-kinesis-data-streams-1.0.0.jar
ENTRYPOINT java -jar aws-fargate-server-sent-events-sse-amazon-kinesis-data-streams-1.0.0.jar -u $url -s $stream -r $region -rms $reportMS -h $headers -t $collectTypes -rt $readtimeoutMS