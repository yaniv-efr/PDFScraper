package com.local;

import com.local.AWS;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.InstanceStateName;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StartInstancesResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

import com.amazonaws.services.sqs.AmazonSQSVirtualQueuesClientBuilder;
import com.amazonaws.services.sqs.util.AbstractAmazonSQSClientWrapper;

import java.util.UUID;
import java.util.List;
import software.amazon.awssdk.services.ec2.model.Instance;
import java.util.HashMap;
import java.util.Map;

public class local {
        private static AWS aws;

    public static void main(String[] args) {
    //checks if ec2 instance is running with ami id ami-0166fe664262f664c using AWS
        aws = AWS.getInstance();
        List <Instance> instances = aws.getAllInstances();
        //checks if the instance with same ami id is running
        String instanceId = "";
        for (Instance instance : instances) {
            if (instance.imageId().equals("ami-0453ec754f44f9a4a")) {
                instanceId = instance.instanceId();
                System.out.println("Instance found");
                break;
            }
        }
        if (instanceId.equals("")) {
            System.out.println("Instance not found");
            //start the instance with same ami id
                aws.runInstanceFromAMI("ami-0453ec754f44f9a4a");
            return;
        }
        Ec2Client ec2 = Ec2Client.builder()
                .region(Region.US_EAST_1)
                .build();
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();
        DescribeInstancesResponse response = ec2.describeInstances(request);
        String fileId = UUID.randomUUID().toString();


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
        try {
                //create temporary queue
                Map<QueueAttributeName, String> attributes = new HashMap<>();
                attributes.put(QueueAttributeName.MESSAGE_RETENTION_PERIOD, "600"); // 10 minutes retention
                attributes.put(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS, "0"); // No long polling

                CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                        .queueName("TemporaryQueue-" + UUID.randomUUID())
                        .attributes(attributes)
                        .build();

                String tempQueueUrl = sqsClient.createQueue(createQueueRequest).queueUrl();
                System.out.println("Created temporary queue: " + queueUrl);
                //send message to manager (s3 url of text of pdfs to act on) (n) (url of temporary queue)
                String message = fileId + " " + "9" + " " + tempQueueUrl;
                SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageBody(message)
                        .delaySeconds(5)
                        .build();
                SendMessageResponse sendMsgResponse = sqsClient.sendMessage(sendMsgRequest);
                System.out.println("Message sent");

                // DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                //         .queueUrl(tempQueueUrl)
                //         .build();
                // sqsClient.deleteQueue(deleteQueueRequest);
                // System.out.println("Deleted temporary queue: " + tempQueueUrl);

        } catch (SqsException e) {
                System.err.println(e.awsErrorDetails().errorMessage());
                System.exit(1);
        }
    }

}
