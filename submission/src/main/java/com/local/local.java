package com.local;

import com.local.AWS;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.InstanceStateName;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StartInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.InstanceType;
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
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;


import com.amazonaws.services.sqs.AmazonSQSVirtualQueuesClientBuilder;
import com.amazonaws.services.sqs.util.AbstractAmazonSQSClientWrapper;

import java.util.UUID;
import java.util.List;
import software.amazon.awssdk.services.ec2.model.Instance;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;


public class local {
        private static AWS aws;

    public static void main(String[] args) {
    //checks if ec2 instance is running with ami id ami-0166fe664262f664c using AWS
        aws = AWS.getInstance();
    List<Instance> instances = aws.getAllInstances();

    // Find instances with the specific AMI ID
    String amiId = "ami-0de8969e53f41f97b";
    List<Instance> matchingInstances = instances.stream()
            .filter(instance -> instance.imageId().equals(amiId))
            .filter(instance -> instance.state().name().equals(InstanceStateName.RUNNING) || instance.state().name().equals(InstanceStateName.PENDING))
            .toList();
        if (matchingInstances.size() == 0) {
                System.out.println("Starting EC2 instance");
                aws.runInstanceFromAmiWithScript(amiId, InstanceType.T2_MICRO , 1 , 1  , "#cloud-boothook\n#!/bin/bash\njava -jar /home/ec2-user/Manager-1.0-SNAPSHOT.jar" );
        } else {
                System.out.println("EC2 instance already running");
        }
        //upload args[0] to s3
        String name = upload(args[0], "tomanager");
        
        //send message to sqs
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/975050155862/LocaltoManager.fifo";
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
                String message = name + " " + args[2] + " " + tempQueueUrl;
                SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageBody(message)
                        .messageGroupId("1")
                        .build();
                SendMessageResponse sendMsgResponse = sqsClient.sendMessage(sendMsgRequest);
                System.out.println("Message sent");
                List<Message> messages = new ArrayList<>();
                while(true){
                        try {
                                Thread.sleep(5000);
                        } catch (InterruptedException e) {
                                e.printStackTrace();
                        }
                        
                        Boolean isMessageReceived = aws.getQueueSize(tempQueueUrl) > 0;
                        if(isMessageReceived){
                                System.out.println("Message received from temporary queue");
                                messages = aws.receiveMessage(tempQueueUrl);
                                break;
                        }
                }
                String msg = messages.get(0).body();
                if (messages.size() > 0) {
                        msg = messages.get(0).body();
                        System.out.println("Received message from temporary queue: " + msg);
                } else {
                        System.out.println("No message received from temporary queue");
                }
                // find a file named msg in tomanager bucket and download it and save under args[2]
                File file = new File(args[1] + ".html");
                aws.downloadFileFromS3(msg , file);

                System.out.println("Downloaded file: " + msg);


                //delete temporary queue
                DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                        .queueUrl(tempQueueUrl)
                        .build();
                sqsClient.deleteQueue(deleteQueueRequest);
                System.out.println("Deleted temporary queue: " + tempQueueUrl);
                //check args length
                if(args.length > 3 && args[3].equals("terminate")){
                        SendMessageRequest sendMsgRequest2 = SendMessageRequest.builder()
                                .queueUrl(queueUrl)
                                .messageBody("terminate")
                                .messageGroupId("1")
                                .build();
                        SendMessageResponse sendMsgResponse2 = sqsClient.sendMessage(sendMsgRequest2);
                }

        } catch (SqsException e) {
                System.err.println(e.awsErrorDetails().errorMessage());
                System.exit(1);
        }
    }

    public static String upload(String file , String bucket){
        String name = UUID.randomUUID().toString();
        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();
        s3.putObject(PutObjectRequest.builder()
                .bucket(bucket)
                .key(name)
                .build(),
                // Path to the file to upload
                java.nio.file.Paths.get(file));//////////////////////replace with reading of args
        System.out.println("Object uploaded");
        return name;
    }


}
