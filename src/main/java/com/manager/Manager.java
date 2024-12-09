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
            			
            			if(msg.body()=="terminate") {
            				waitingForTermination=true;
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
                    		if(activeWorkers+workersToCreate>maxWorkers) {
                    			workersToCreate = maxWorkers-activeWorkers;
                    		}	
							if(workersToCreate>0) {
								activeWorkers+=workersToCreate;
								aws.runInstanceFromAmiWithScript("ami-0c8ba19b357bd8ab1", InstanceType.T2_NANO, workersToCreate, workersToCreate, "#cloud-boothook\n#!/bin/bash\njava -jar /home/ec2-user/worker-1.0-SNAPSHOT.jar");
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
            					
            				if(taskManagement.get(client).contains(task)) {
            				File outputFile = new File(client+".html");
            				try {
								BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile , true));
								writer.write("<"+operation+">"+originalFile+" "+proccessedFile);
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
        for(Instance inst:aws.getAllInstances()) {
			aws.terminateInstance(inst.instanceId());
		}
    }

    public static void main(String[] a){
        Manager manager = new Manager();
        manager.start();
    }
}