package uk.co.odinconsultants;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.*;

/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 *
 * For more information, see the following documentation topic:
 *
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 *
 * Code from https://docs.aws.amazon.com/AmazonS3/latest/userguide/example_s3_DeleteObjects_section.html
 */
public class DeleteMultiObjects {
    public static void main(String[] args) {
        final String usage = "Usage:    <bucketName>\nWhere:\nbucketName - the Amazon S3 bucket name.\n";

        if (args.length != 1) {
            System.out.println(usage);
            System.exit(1);
        }

        String bucketName = args[0];

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Region.EU_WEST_2.id())
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();

        Collection<String> keys = getObjectNamesIn(s3Client, bucketName);
        deleteObjectsMatching(keys, s3Client, bucketName, "default");

    }

    private static void deleteObjectsMatching(Collection<String> keys, AmazonS3 s3Client, String bucketName, String substring) {
        for (String key : keys) {
            if (key.contains(substring)) {
                System.out.println("Deleting " + key);
                s3Client.deleteObject(bucketName, key);
            }
        }
    }

    public static Collection<String> getObjectNamesIn(AmazonS3 s3Client, String bucketName) {
        Set<String> keys = new HashSet<>();
        ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName);
        ListObjectsV2Result result;
        do {
            result = s3Client.listObjectsV2(request);
            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                String key = objectSummary.getKey();
                System.out.println("ObjectIdentifier: " + key);
                keys.add(key);
            }
            // Set the continuation token to paginate through the results if necessary
            request.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated());
        return keys;
    }


    public static Collection<ObjectIdentifier> listBucketObjects(S3Client s3, String bucketName) throws S3Exception {
        Set<ObjectIdentifier> keys = new HashSet<>();

        ListObjectsRequest listObjects = ListObjectsRequest
                .builder()
                .bucket(bucketName)
                .build();

        ListObjectsResponse res = s3.listObjects(listObjects);
        List<S3Object> objects = res.contents();
        for (S3Object myValue : objects) {
            System.out.print("\n The name of the key is " + myValue.key());
            System.out.print("\n The owner is " + myValue.owner());
        }

        return keys;
    }

    public static void deleteBucketObjects(S3Client s3, String bucketName, Collection<ObjectIdentifier> keys) {
        // Delete multiple objects in one request.
        Delete del = Delete.builder()
                .objects(keys)
                .build();

        try {
            DeleteObjectsRequest multiObjectDeleteRequest = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(del)
                    .build();

            s3.deleteObjects(multiObjectDeleteRequest);
            System.out.println("Multiple objects are deleted!");

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}


