package com.manager;
import API.AWS;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.awt.Label;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Executable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.Collections;

import com.amazonaws.services.sqs.AmazonSQSVirtualQueuesClientBuilder;

public class Manager{
    private boolean  terminated;
    private boolean  waitingForTermination;
    private AWS aws;
    private Executor tp;
    private int maxWorkers;
    private int activeWorkers;
    private HashMap<String, List<String>> taskManagement;
    private HashMap<String, String> returnURls;
    int clientCounter;
    
    public Manager(){
        terminated = false;
        waitingForTermination = false;
        aws = AWS.getInstance();
        tp = Executors.newFixedThreadPool(10);
        maxWorkers=10;
        activeWorkers=0;
        taskManagement=new HashMap<>();
        returnURls = new HashMap<>();
        clientCounter=0;
    }
    public void start() {
    	
    	aws.createQueue("ManagerToWorkers");
    	aws.createQueue("WorkersToManager");
		aws.createQueue("LocaltoManager");
    	String clientsQueueURl = aws.getQueueUrl("LocaltoManager");
    	String workersQueueURl = aws.getQueueUrl("ManagerToWorkers");
    	String workersResponseQueueURl = aws.getQueueUrl("WorkersToManager");
        while(!terminated){
            if(aws.getQueueSize(clientsQueueURl)>0&&!waitingForTermination){
            	tp.execute(() -> {
            		List<Message> messageLst = aws.receiveMessage(clientsQueueURl);
            		if(!messageLst.isEmpty()) {
            			Message msg = messageLst.get(0);

            			if(msg.body().equals("terminate")) {
            				waitingForTermination=true;
							if(taskManagement.isEmpty()) {
								terminated=true;
							}
            			}else {
                    	String taskPath = msg.body().split(" ")[0];
                    	int n =Integer.parseInt(msg.body().split(" ")[1]);
                    	String returnURL = msg.body().split(" ")[2];
                    	clientCounter++;
                    	String sender = ""+clientCounter;
                    	returnURls.put(sender, returnURL);
                    	File taskFile = new File(sender);
                    	aws.downloadFileFromS3(taskPath, taskFile);
                    	aws.deleteMessage(clientsQueueURl, msg.receiptHandle());
                    	int workerTasksCounter=0;
                    	try {
                    		BufferedReader reader = new BufferedReader(new FileReader(taskFile));
                    		String line;
                    		List<String> workerMsgs = new ArrayList<>();
                    		while ((line = reader.readLine())!=null) {
								//create line but replace " " with ","
								line = line + " " + sender;
								line = line.replace(" ", ",");
								
								line = line.replace("	", ",");
                    			workerTasksCounter++;
                    			workerMsgs.add(line); 
                    			if(taskManagement.isEmpty()||!taskManagement.keySet().contains(sender)) {
                    				taskManagement.put(sender, Collections.synchronizedList(new ArrayList<String>()));
                    			}
                    			taskManagement.get(sender).add(line);
                    		}
                    		reader.close();
                    		taskFile.delete();
                    		int workersToCreate = workerTasksCounter/n;
							if(workersToCreate==0) {
								workersToCreate=1;
							}
                    		if(activeWorkers+workersToCreate>maxWorkers) {
                    			workersToCreate = maxWorkers-activeWorkers;
                    		}	
							if(workersToCreate>0) {
								activeWorkers+=workersToCreate;
								aws.runInstanceFromAmiWithScript("ami-01fea53c76173b9be", InstanceType.T2_NANO, workersToCreate, workersToCreate, "#cloud-boothook\n#!/bin/bash\njava -jar /home/ec2-user/worker-2.0-SNAPSHOT.jar");
                    		}for(String wrkmsg:workerMsgs) {
                    			tp.execute(()->aws.sendMessage(workersQueueURl, wrkmsg));
                    			
                    		}
                    		
                    		
                    		System.out.println(workerTasksCounter);
                    		
                    	}catch(FileNotFoundException ex) {}
                    	catch(IOException ex) {}
            			}
            		}
                	
            	});
            	
            }
            if(aws.getQueueSize(workersResponseQueueURl)>0) {
            	tp.execute(()->{
            		List<Message> messageLst = aws.receiveMessage(workersResponseQueueURl);
            		if(!messageLst.isEmpty()) {
            			Message msg = messageLst.get(0);
            			String proccessedFile = msg.body().split(" ")[0];
            			String operation = msg.body().split(" ")[1];
            			String originalFile = msg.body().split(" ")[2];
            			String client=msg.body().split(" ")[3];
            			String task = operation+","+originalFile+","+client;
            			aws.deleteMessage(workersResponseQueueURl, msg.receiptHandle());
            				System.out.println("client: "+client + " task: "+task);
            				if(taskManagement.get(client).contains(task)) {
            				File outputFile = new File(client+".html");
            				try {
								//if proccessed file starts with Error:<number> add the description of the error to the output file
								if(proccessedFile.startsWith("Error:")) 
									proccessedFile = proccessedFile + " " + getHttpErrorDescription(Integer.parseInt(proccessedFile.split(":")[1]));

								BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile , true));
								writer.write(operation+":  "+originalFile+"    "+proccessedFile + "<br>");
								writer.flush();
								writer.close();
								taskManagement.get(client).remove(task);
								
									
								if(taskManagement.get(client).isEmpty()) {
									taskManagement.remove(client);
									aws.uploadFileToS3(client, outputFile);
									outputFile.delete();
									aws.sendMessage(returnURls.get(client), client);
								}
								if(waitingForTermination&&taskManagement.isEmpty()) {
									terminated=true;
								}
									
									
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
            				
            			}
            		}
            	});
            }
        }
		//get all worker instances via ami
		//terminate all worker instances
        List<Instance> instances = aws.getAllInstances();
		List<Instance> matchingInstances = instances.stream()
            .filter(instance -> instance.imageId().equals("ami-01fea53c76173b9be"))
            .toList();
		for(Instance instance:matchingInstances) {
			if(instance.state().name().toString().equals("running"))
				aws.terminateInstance(instance.instanceId());
		}
		for(Instance instance:aws.getAllInstances()) {
			if(instance.state().name().toString().equals("running"))
			{aws.terminateInstance(instance.instanceId());}
		}
		return;
    }
	//System.err.println("Domain expired or host not found: " + e.getMessage());
    //     return "Error: Domain-expired-or-host-not-found";
    // } catch (SocketTimeoutException e) {
    //     System.err.println("Connection timed out: " + e.getMessage());
    //     return "Error: Connection-timed-out";
    // } catch (IOException e) {
    //     System.err.println("IO Error: " + e.getMessage());
    //     return "Error: IO-Error";
	//give each error a code
	public static String getHttpErrorDescription(int code) {
		switch(code) {
			case 991:
				return "Domain-expired-or-host-not-found";
			case 992:
				return "Connection-timed-out";
			case 993:
				return "IO-Error";
			case 994:
				return "invalid-pdf-file";
			case 300:
				return "Multiple-Choices";
			case 301:
				return "Moved-Permanently";
			case 400:
				return "Bad-Request";
			case 401:
				return "Unauthorized";
			case 403:
				return "Forbidden";
			case 404:
				return "Not-Found";
			default:
				return "Unknown-Error";
		}
	}

    public static void main(String[] a){
        Manager manager = new Manager();
        manager.start();
		
    }
}