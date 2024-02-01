package uk.co.odinconsultants;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import software.amazon.awssdk.regions.Region;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

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
public class S3Utils {
    public static void main(String[] args) {
        final String usage = "Usage:    <bucketName>\nWhere:\nbucketName - the Amazon S3 bucket name.\n";

        if (args.length != 1) {
            System.out.println(usage);
            System.exit(1);
        }

        String bucketName = args[0];
        S3Utils app = new S3Utils(bucketName);
        Collection<String> keys = app.getObjectNamesIn(bucketName);
        app.deleteObjectsMatching(keys, bucketName, "default");
        app.deleteObjectsMatching(keys, bucketName, "output");
    }

    private final AmazonS3 s3Client;

    public S3Utils(String bucketName) {
        s3Client = AmazonS3ClientBuilder.standard().withRegion(Region.EU_WEST_2.id())
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();
    }

    public void deleteObjectsMatching(Collection<String> keys, String bucketName, String substring) {
        for (String key : keys) {
            if (key.contains(substring)) {
                System.out.println("Deleting " + key);
                s3Client.deleteObject(bucketName, key);
            }
        }
    }

    public Collection<String> getObjectNamesIn(String bucketName) {
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

}


