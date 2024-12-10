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

import com.amazonaws.services.sqs.AmazonSQSVirtualQueuesClientBuilder;

public class Manager{
    private boolean  terminated;
    private boolean  waitingForTermination;
    private AWS aws;
    private Executor tp;
    private int maxWorkers;
    private int activeWorkers;
    private HashMap<String, Set<String>> taskManagement;
    private HashMap<String, String> returnURls;
    int clientCounter;
    private final SqsClient tsqs;
    private final SqsClient vsqs;
    
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
        tsqs = SqsClient.builder().region(Region.US_EAST_1).build();
        vsqs = AmazonSQSVirtualQueuesClientBuilder.standard().withAmazonSQS(tsqs).build();
    }
    public void start() {
    	
    	aws.createQueue("ManagerToWorkers");
    	aws.createQueue("WorkersToManager");
    	String clientsQueueURl = getQueueUrl("placeholder");
    	String workersQueueURl = aws.getQueueUrl("ManagerToWorkers");
    	String workersResponseQueueURl = aws.getQueueUrl("WorkersToManager");
        while(!terminated){
            if(getQueueSize(clientsQueueURl)>0&&!waitingForTermination){
            	tp.execute(() -> {
            		List<Message> messageLst = receiveMessage(clientsQueueURl);
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
                    	deleteMessage(clientsQueueURl, msg.receiptHandle());
                    	int workerTasksCounter=0;
                    	try {
                    		BufferedReader reader = new BufferedReader(new FileReader(taskFile));
                    		String line;
                    		List<String> workerMsgs = new ArrayList<>();
                    		while ((line = reader.readLine())!=null) {
                    			workerTasksCounter++;
                    			workerMsgs.add(line+" "+sender); 
                    			if(taskManagement.isEmpty()||!taskManagement.keySet().contains(sender)) {
                    				taskManagement.put(sender, new HashSet<String>());
                    			}
                    			taskManagement.get(sender).add(line);
                    		}
                    		reader.close();
                    		taskFile.delete();
                    		int workersToCreate = workerTasksCounter/n;
                    		if(activeWorkers+workersToCreate>maxWorkers) {
                    			workersToCreate = maxWorkers-activeWorkers;
                    		}
                    		
                    		//aws.runInstanceFromAmiWithScript("image", InstanceType.T2_NANO, workersToCreate, workersToCreate, "script");
                    		
                    		for(String wrkmsg:workerMsgs) {
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
            			String task = operation+" "+originalFile;
            			aws.deleteMessage(workersResponseQueueURl, msg.receiptHandle());
            					
            				if(taskManagement.get(client).contains(task)) {
            				File outputFile = new File(client+".html");
            				try {
								BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
								writer.write("<"+operation+">"+originalFile+" "+proccessedFile);
								writer.close();
								taskManagement.get(client).remove(task);
								
									
								if(taskManagement.get(client).isEmpty()) {
									taskManagement.remove(client);
									aws.uploadFileToS3(client, outputFile);
									outputFile.delete();
									sendMessage(returnURls.get(client), client);
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
    public String getQueueUrl(String queueName) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String queueUrl = null;
        queueUrl = vsqs.getQueueUrl(getQueueRequest).queueUrl();
        System.out.println("Queue URL: " + queueUrl);
        return queueUrl;
    }

    public int getQueueSize(String queueUrl) {
        GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED
                )
                .build();

        GetQueueAttributesResponse queueAttributesResponse = null;
        queueAttributesResponse = vsqs.getQueueAttributes(getQueueAttributesRequest);
        Map<QueueAttributeName, String> attributes = queueAttributesResponse.attributes();

        return Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)) +
                Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)) +
                Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED));
    }
    public List<Message> receiveMessage(String queueURL) {
    	ReceiveMessageRequest req = ReceiveMessageRequest.builder()
    			.queueUrl(queueURL)
    			.maxNumberOfMessages(1)
    			.build();
    	ReceiveMessageResponse res = vsqs.receiveMessage(req);
    	return res.messages();
    }
    public void sendMessage(String queueURL,String msg) {
    	SendMessageRequest req = SendMessageRequest.builder()
    			.messageBody(msg)
    			.queueUrl(queueURL)
    			.build();
    	vsqs.sendMessage(req);
    }
    public void deleteMessage(String queueURL ,String receiptHandle) {
    	DeleteMessageRequest req = DeleteMessageRequest.builder()
    			.queueUrl(queueURL)
    			.receiptHandle(receiptHandle)
    			.build();
    	vsqs.deleteMessage(req);
    }
    
}