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


public class local {
        private static AWS aws;

    public static void main(String[] args) {
    //checks if ec2 instance is running with ami id ami-0166fe664262f664c using AWS
        aws = AWS.getInstance();
    List<Instance> instances = aws.getAllInstances();

    // Find instances with the specific AMI ID
    String amiId = "ami-0453ec754f44f9a4a";
    List<Instance> matchingInstances = instances.stream()
            .filter(instance -> instance.imageId().equals(amiId))
            .filter(instance -> instance.state().name().equals(InstanceStateName.RUNNING) || instance.state().name().equals(InstanceStateName.PENDING))
            .toList();

        //upload args[0] to s3
        String name = upload(args[0], "tomanager");
        
        //send message to sqs
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/975050155862/LocaltoManager";
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
                        .delaySeconds(5)
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
                if (messages.size() > 0) {
                        System.out.println("Received message from temporary queue: " + messages.get(0).body());
                } else {
                        System.out.println("No message received from temporary queue");
                }
                // find all file in s3 bucket resultbucket-aws1 starting with 1/
                S3Client s3Client = S3Client.builder()
                        .region(Region.US_EAST_1)
                        .build();

                List<String> urls = new ArrayList<>();
                ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                        .bucket("resultbucket-aws1")
                        .prefix("1/")
                        .build();
                ListObjectsV2Response listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);
                for (S3Object object : listObjectsV2Response.contents()) {
                        urls.add(object.key());
                }
                //generate completion html
                generateCompleteionHTML(urls);


                DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                        .queueUrl(tempQueueUrl)
                        .build();
                sqsClient.deleteQueue(deleteQueueRequest);
                System.out.println("Deleted temporary queue: " + tempQueueUrl);
                //send termination message to manager
                SendMessageRequest sendMsgRequest2 = SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageBody("terminate")
                        .delaySeconds(5)
                        .build();
                SendMessageResponse sendMsgResponse2 = sqsClient.sendMessage(sendMsgRequest2);

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

    public static void generateCompleteionHTML (List<String> urls){
        StringBuilder htmlContent = new StringBuilder();
        htmlContent.append("<!DOCTYPE html><html><head><title>PDF to HTML</title></head><body>");
        htmlContent.append("<h1>PDF Content - First Page</h1>");
        htmlContent.append("<div style=\"white-space: pre-wrap;\">");
        for (String url : urls) {
            htmlContent.append("<a href=\"" + url + "\">" + url + "</a><br>");
        }
        htmlContent.append("</div></body></html>");
        try (FileWriter writer = new FileWriter("completion.html")) {
            writer.write(htmlContent.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
