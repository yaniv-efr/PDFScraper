Yaniv Efremov 324265016
Guy Meir 211789862

running the project: start the aws session and update the .aws/credentials file. than navigate to the local.jar file and run the command: java -jar local-1.0-SNAPSHOT.jar inputFileName outputFileName n [terminate]

system overview: the local application check if a manager is running and if not it start one.
than the local upload the input file to s3, create a temporary sqs queue and send a message on the LocalToManager sqs queue.
when the manager receive a massage from the local application it it download the input file from the s3 and create the workers
according to the size of the file, the amount of active workers, the parameter n, and the max number of workers allowed(currently 10).
than the manager send the tasks to the ManagerToWorker sqs queue.
when the workers receive the tasks each one download the pdf file, process the file according to the task upload the processed file to the s3 and send the URL back to the manager.
after the manager receive the result of a task it update the output file and mark the task as fulfilled. when the last task is fulfilled the manager upload the ouput file to s3 and send its URL to the local application via the temporary queue. the local download the output file to the outputFileName path and than send terminate message if requested by the user

ec2 types: manager - t2.micro
	   worker - t2.nano

the example of input file input-sample-1.txt: with parameter n=9 it 7 minutes for the program to finish
