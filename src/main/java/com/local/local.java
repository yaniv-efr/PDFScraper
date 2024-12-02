package com.local;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.InstanceStateName;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StartInstancesResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import java.util.UUID;

public class local {

    public static void main(String[] args) {
    //checks if ec2 instance is running with id i-01a27791f67ae7a62
        String instanceId = "i-01a27791f67ae7a62";
        Region region = Region.US_EAST_1;
        String fileId = UUID.randomUUID().toString();
        Ec2Client ec2 = Ec2Client.builder()
                .region(region)
                .build();

        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        DescribeInstancesResponse response = ec2.describeInstances(request);
        if (response.reservations().size() > 0) {
            if (response.reservations().get(0).instances().size() > 0) {
                if (response.reservations().get(0).instances().get(0).state().name().equals(InstanceStateName.RUNNING)) {
                    System.out.println("Instance is running");
                } else {
                    System.out.println("Instance is not running");
                    StartInstancesRequest startRequest = StartInstancesRequest.builder()
                            .instanceIds(instanceId)
                            .build();
                    StartInstancesResponse startResponse = ec2.startInstances(startRequest);
                    System.out.println("Instance started");
                }
            }
        }
        //upload args[0] to s3
        // Create an S3Client object
        // Create an S3Client object
        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();
        // Upload a file as a new object with ContentType and title specified
        String bucket = "tomanager";
        s3.putObject(PutObjectRequest.builder()
                .bucket(bucket)
                .key(fileId)
                .build(),
                // Path to the file to upload
                java.nio.file.Paths.get("one.txt"));
        System.out.println("Object uploaded");
        //send message to sqs
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/975050155862/local-manager";
        // Create the SQS client
        SqsClient sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();
        // Send a message to the queue
        SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(fileId)
                .delaySeconds(5)
                .build();
        sqsClient.sendMessage(sendMsgRequest);
        System.out.println("Message sent to the queue");
        //wait for the manager's sqs message

        

    }
}
