package com.example;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.InstanceStateName;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StartInstancesResponse;

public class EC2InstanceManager {

    public static void main(String[] args) {
        // Replace with your instance ID
        String instanceId = "i-0f5749b5f9f5d8788";
        // Specify the AWS region
        Region region = Region.US_EAST_1;

        // Create the EC2 client
        try (Ec2Client ec2 = Ec2Client.builder().region(region).build()) {
            if (isInstanceStopped(ec2, instanceId)) {
                startInstance(ec2, instanceId);
            } else {
                System.out.println("Instance " + instanceId + " is not in the 'stopped' state. No action taken.");
            }
        }
    }

    /**
     * Check if the EC2 instance is in the 'stopped' state.
     */
    private static boolean isInstanceStopped(Ec2Client ec2, String instanceId) {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        DescribeInstancesResponse response = ec2.describeInstances(request);
        String state = response.reservations()
                .get(0)
                .instances()
                .get(0)
                .state()
                .nameAsString();

        System.out.println("Instance " + instanceId + " is in state: " + state);
        return InstanceStateName.STOPPED.toString().equalsIgnoreCase(state);
    }

    /**
     * Start the EC2 instance.
     */
    private static void startInstance(Ec2Client ec2, String instanceId) {
        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        StartInstancesResponse response = ec2.startInstances(request);
        System.out.println("Starting instance " + instanceId + ". Current state: " +
                response.startingInstances().get(0).currentState().nameAsString());
    }
}
