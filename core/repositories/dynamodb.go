package repositories

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func AWSConfig() (aws.Config, error) {
	awsRegion := os.Getenv("AWS_REGION")
	awsEndpoint := os.Getenv("AWS_ENDPOINT")

	awsRegion = "us-west-1"
	awsEndpoint = "http://localhost:4566"

	credentials := credentials.StaticCredentialsProvider{
		Value: aws.Credentials{
			AccessKeyID: "123", SecretAccessKey: "xyz",
			Source: "Hard-coded credentials; values are irrelevant for local DynamoDB",
		},
	}

	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if awsEndpoint != "" {
			return aws.Endpoint{
				URL:           awsEndpoint,
				SigningRegion: awsRegion,
			}, nil
		}

		// returning EndpointNotFoundError will allow the service to fallback to its default resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(awsRegion),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials),
		config.WithClientLogMode(aws.LogRequest|aws.LogRetries))

	if err != nil {
		log.Fatalf("Failed to load SDK Config, %v", err)
		return aws.Config{}, err
	}

	return awsCfg, nil
}
