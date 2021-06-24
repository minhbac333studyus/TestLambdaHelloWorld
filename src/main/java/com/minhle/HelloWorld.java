package com.minhle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.partitions.model.Region;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.amazonaws.services.simpleemail.model.Body;
import com.amazonaws.services.simpleemail.model.Content;
import com.amazonaws.services.simpleemail.model.Destination;
import com.amazonaws.services.simpleemail.model.Message;
import com.amazonaws.services.simpleemail.model.SendEmailRequest;
class S3Model{
    String idName;
    String Food;
    S3Model(String idName, String Food){
        this.idName = idName;
        this.Food = Food;
    }
}

class MostModified implements Comparator<S3ObjectSummary> {


    public int compare(S3ObjectSummary o1,S3ObjectSummary o2) {
        if ( o1.getLastModified().getTime() > o2.getLastModified().getTime())
            return -1;
        else if ( o1.getLastModified().getTime() < o2.getLastModified().getTime())
            return 1;
        else return 0;
    }

}
public class HelloWorld implements RequestHandler<S3Event, String> {
    public BasicAWSCredentials credentials;
    private AmazonS3 s3;
    private final String REGION = "us-east-2";
    private final String bucketName= "s3.bucket11";
    private static AmazonDynamoDB dynamoDB;

    @Override
    public String handleRequest(S3Event event, Context context) {
        context.getLogger().log("Received event: " + event);
        setUpCredential();
        setUpS3Client();
        set_up_DynamoDB_Client();
        //controlS3(bucketName);
        addItemAdam("firstDBtable", getTheMostRecentFile(bucketName));
        sendEmailBySES(credentials);
        // Get the object from the event and show its content type
        return "Hello from Lambda!";
    }

    private void setUpS3Client() {
        this.s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(REGION)
                .build();
    }
    private void set_up_DynamoDB_Client() {
        dynamoDB = AmazonDynamoDBClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(REGION)
                .build();

    }
    private void setUpCredential() {
        this.credentials= new BasicAWSCredentials(
                "xxxx",
                "f9YUjnFRRJvyUIAsX7ciaESg+Y8mtDpKjX99Fw51");
        try {
            credentials.getAWSAccessKeyId();
            credentials.getAWSSecretKey();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (C:\\Users\\adam\\.aws\\credentials), and is in valid format.",
                    e);
        }
    }
    private static Map<String, AttributeValue> newItemAdam(S3Model s3model) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("IdName", new AttributeValue(s3model.idName));
        item.put("Food", new AttributeValue(s3model.Food) );
        return item;
    }
    private static void addItemAdam(String tableName,S3Model s3model) {
        Map<String, AttributeValue> item = newItemAdam(s3model);
        PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
        PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
        System.out.println("Result: " + putItemResult);
    }
    private S3Model getTheMostRecentFile(String bucketName){
        S3Model returnItem = new S3Model("No","Nah");

        ObjectListing obList = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
        List<S3ObjectSummary> allItem = obList.getObjectSummaries();
        Collections.sort(allItem,new MostModified());
        S3ObjectSummary mostRecentItem = allItem.get(0);
        String nameFile = mostRecentItem.getKey();
        String fileType = mostRecentItem.toString();
        returnItem = new S3Model(nameFile, fileType);
        return returnItem;
    }

    private void controlS3(String bucketName) {
        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon S3");
        System.out.println("===========================================\n");
        try {
            System.out.println("Listing objects");
            ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                    .withBucketName(bucketName) );
            S3ObjectSummary recent = new S3ObjectSummary();

            long max = 0;

            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                if(max < objectSummary.getLastModified().getTime()) {
                    recent = objectSummary;
                    max = objectSummary.getLastModified().getTime();
                }
            }
            System.out.println(" Recent item is " + recent.getKey());
            System.out.println();
        }
        catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
    private void sendEmailBySES(BasicAWSCredentials credentials) {
        String FROM = "minhbac333@gmail.com";  // Replace with your "From" address. This address must be verified.
        String TO = "minhbac333studyus@gmail.com"; // Replace with a "To" address. If you have not yet requested
        // production access, this address must be verified.
        String fileName = getTheMostRecentFile(bucketName).idName;
        String BODY = "The file "+ fileName + " was uploaded on S3";
        String SUBJECT = "Amazon SES ";
        Destination destination = new Destination().withToAddresses(new String[]{TO});

        // Create the subject and body of the message.
        Content subject = new Content().withData(SUBJECT);
        Content textBody = new Content().withData(BODY);
        Body body = new Body().withText(textBody);

        // Create a message with the specified subject and body.
        Message message = new Message().withSubject(subject).withBody(body);

        // Assemble the email.
        SendEmailRequest request = new SendEmailRequest().withSource(FROM).withDestination(destination).withMessage(message);

        try {
            System.out.println("Attempting to send an email through Amazon SES by using the AWS SDK for Java...");

            // Instantiate an Amazon SES client, which will make the service call with the supplied AWS credentials.
            AmazonSimpleEmailService client = AmazonSimpleEmailServiceClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withRegion("us-east-2")
                    .build();

            // Send the email.
            client.sendEmail(request);
            System.out.println("Email sent!");

        } catch (Exception ex) {
            System.out.println("The email was not sent.");
            System.out.println("Error message: " + ex.getMessage());
        }
    }


}
