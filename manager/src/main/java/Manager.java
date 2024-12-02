import API.AWS;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.sqs.model.Message;

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
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class Manager{
    private boolean  terminated;
    private boolean  waitingForTermination;
    private AWS aws;
    private Executor tp;
    private int maxWorkers;
    private int activeWorkers;
    private HashMap<String, Set<String>> taskManagement;
    int clientCounter;
    
    public Manager(){
        terminated = false;
        waitingForTermination = false;
        aws = AWS.getInstance();
        tp = Executors.newFixedThreadPool(10);
        maxWorkers=10;
        activeWorkers=0;
        taskManagement=new HashMap<>();
        clientCounter=0;
    }
    public void start() {
    	String clientsQueueURl = aws.getQueueUrl("placeholder");
    	String workersQueueURl = aws.getQueueUrl("placeholder2");
    	String workersResponseQueueURl = aws.getQueueUrl("placeholder3");
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
                    	String sender = ""+(clientCounter++);
                    	File taskFile = new File(sender);
                    	aws.downloadFileFromS3(taskPath, taskFile);
                    	aws.deleteMessage(clientsQueueURl, msg.receiptHandle());
                    	int workerTasksCounter=0;
                    	try {
                    		BufferedReader reader = new BufferedReader(new FileReader(taskFile));
                    		String line;
                    		List<String> workerMsgs = new ArrayList<>();
                    		while ((line = reader.readLine())!=null) {
                    			workerTasksCounter++;
                    			workerMsgs.add(line+" "+sender); 
                    			if(!taskManagement.keySet().contains(sender)) {
                    				taskManagement.put(sender, new HashSet<String>());
                    			}
                    			taskManagement.get(sender).add(line);
                    		}
                    		reader.close();
                    		int workersToCreate = workerTasksCounter/n;
                    		if(activeWorkers+workersToCreate>maxWorkers) {
                    			workersToCreate = maxWorkers-activeWorkers;
                    		}
                    		
                    		aws.runInstanceFromAmiWithScript("image", InstanceType.T2_NANO, workersToCreate, workersToCreate, "script");
                    		
                    		for(String wrkmsg:workerMsgs) {
                    			tp.execute(()->aws.sendMessage(workersQueueURl, wrkmsg));
                    			for(Instance inst:aws.getAllInstances()) {
                    				aws.terminateInstance(inst.instanceId());
                    			}
                    			
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
            					
            				if(taskManagement.get(client).contains(task)) {
            				File outputFile = new File(client+".html");
            				try {
								BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
								writer.write("<"+operation+">"+originalFile+" "+proccessedFile);
								writer.close();
								taskManagement.get(client).remove(task);
								aws.deleteMessage(workersResponseQueueURl, msg.receiptHandle());
									
								if(taskManagement.get(client).isEmpty()) {
									taskManagement.remove(client);
									aws.uploadFileToS3(client, outputFile);
									//send response
								}
								if(waitingForTermination&&taskManagement.isEmpty()) {
									terminated=false;
									
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
    }

    public static void main(String[] a){
        Manager manager = new Manager();
        manager.start();
    }
}
