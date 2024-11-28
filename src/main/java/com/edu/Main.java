package com.edu;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class Main {
    public static void main(String[] args) {
        // Create the SQS client
        SqsClient sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();

        String queueUrl = "https://sqs.us-east-1.amazonaws.com/975050155862/manager-worker"; // Replace with your queue URL

        while (true) {
            try {
                // Receive messages from the queue
                ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(10) // Polling wait time for long polling
                        .build();
                ReceiveMessageResponse receiveResponse = sqsClient.receiveMessage(receiveRequest);
                //print the message
                System.out.println("Received messages: " + receiveResponse.messages().size());
                // Exit if we get kill signal
                
                // Process each received message
                for (Message message : receiveResponse.messages()) {
                    try {
                        System.out.println("Processing message: " + message.body());
                        // Parse the message body
                        String[] messageParts = message.body().split(",");
                        String action = messageParts[0];
                        String fileURL = messageParts[1];
                        String saveDir = "midWork"; // Temporary save directory

                        // Call the PdfWorker
                        PdfWorker.work(action, fileURL, saveDir);

                        // Delete the message from the queue
                        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                                .queueUrl(queueUrl)
                                .receiptHandle(message.receiptHandle())
                                .build();
                        sqsClient.deleteMessage(deleteRequest);

                        System.out.println("Processed and deleted message: " + message.body());
                    } catch (Exception e) {
                        System.err.println("Error processing message: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            } catch (SqsException e) {
                System.err.println("SQS Error: " + e.awsErrorDetails().errorMessage());
                System.exit(1); // Exit if there's a fatal SQS issue
            } catch (Exception e) {
                System.err.println("Unexpected error: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
