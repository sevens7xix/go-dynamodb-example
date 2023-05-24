package repositories

import (
	"context"
	"fmt"
	"go-dynamodb-example/core/domain"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type PartiQLRunner struct {
	Client    *dynamodb.Client
	TableName string
}

func NewPartiQLRunner(name string) PartiQLRunner {
	awsCfg, err := AWSConfig()

	if err != nil {
		log.Fatalf("Failed to load SDK Config, %v", err)
	}

	svc := dynamodb.NewFromConfig(awsCfg)

	return PartiQLRunner{
		Client:    svc,
		TableName: name,
	}
}

func (r PartiQLRunner) AddMovieWithPartiQL(movie domain.Movie) error {
	params, err := attributevalue.MarshalList([]interface{}{movie.Title, movie.Year, movie.Info})

	if err != nil {
		log.Printf("error marshalling the params, %v", err)
		return err
	}

	_, err = r.Client.ExecuteStatement(context.TODO(), &dynamodb.ExecuteStatementInput{
		Statement:  aws.String(fmt.Sprintf("INSERT INTO \"%v\" VALUE {'title': ?, 'year': ?, 'info': ?}", r.TableName)),
		Parameters: params,
	})

	if err != nil {
		log.Printf("error executing the parameters, %v", err)
		return err
	}

	return err
}

func (r PartiQLRunner) GetMovieWithPartiQL(title string, year int) (domain.Movie, error) {
	var movie domain.Movie

	params, err := attributevalue.MarshalList([]interface{}{title, year})
	if err != nil {
		log.Printf("error marshalling the params, %v", err)
		return domain.Movie{}, err
	}

	response, err := r.Client.ExecuteStatement(context.TODO(), &dynamodb.ExecuteStatementInput{
		Statement:  aws.String(fmt.Sprintf("SELECT * FROM \"%v\" WHERE title = ? AND year = ?", r.TableName)),
		Parameters: params,
	})

	if err != nil {
		log.Printf("error execuitng the PartiQL Statement, %v", err)
		return domain.Movie{}, err
	}

	if err := attributevalue.UnmarshalMap(response.Items[0], &movie); err != nil {
		log.Printf("error unmarshalling the response into a movie struct, %v", err)
		return domain.Movie{}, err
	}

	return movie, nil
}

func (r PartiQLRunner) UpdateMovieWithPartiQL(movie domain.Movie, rating float64) error {
	params, err := attributevalue.MarshalList([]interface{}{rating, movie.Title, movie.Year})

	if err != nil {
		log.Printf("error marshalling the params, %v", err)
		return err
	}

	_, err = r.Client.ExecuteStatement(context.TODO(), &dynamodb.ExecuteStatementInput{
		Statement:  aws.String(fmt.Sprintf("UPDATE \"%v\" SET info.rating = ? WHERE title = ? AND year = ?", r.TableName)),
		Parameters: params,
	})

	if err != nil {
		log.Printf("error executing the PartiQL statement, %v", err)
		return err
	}

	return nil
}

func (r PartiQLRunner) DeleteMovieWithPartiQL(title string, year int) error {
	params, err := attributevalue.MarshalList([]interface{}{title, year})

	if err != nil {
		log.Printf("error marshalling the params, %v", err)
		return err
	}

	_, err = r.Client.ExecuteStatement(context.TODO(), &dynamodb.ExecuteStatementInput{
		Statement:  aws.String(fmt.Sprintf("DELETE FROM \"%v\" WHERE title = ? AND year = ?", r.TableName)),
		Parameters: params,
	})

	if err != nil {
		log.Printf("error executing the delete statement with PartiQL, %v", err)
		return err
	}

	return err
}
