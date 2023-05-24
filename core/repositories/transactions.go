package repositories

import (
	"context"
	"go-dynamodb-example/core/domain"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Table struct {
	Client *dynamodb.Client
	Name   string
}

func NewTableClient(tableName string) Table {

	awsCfg, err := AWSConfig()

	if err != nil {
		log.Fatalf("Failed to load SDK Config, %v", err)
	}

	svc := dynamodb.NewFromConfig(awsCfg)

	return Table{
		Client: svc,
		Name:   tableName,
	}
}

func (m Table) CreateTable() error {
	input := &dynamodb.CreateTableInput{
		TableName: aws.String(m.Name),
		AttributeDefinitions: []types.AttributeDefinition{{
			AttributeName: aws.String("year"),
			AttributeType: types.ScalarAttributeTypeN,
		}, {
			AttributeName: aws.String("title"),
			AttributeType: types.ScalarAttributeTypeS,
		}},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("year"),
			KeyType:       types.KeyTypeHash,
		}, {
			AttributeName: aws.String("title"),
			KeyType:       types.KeyTypeRange,
		}},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
	}

	_, err := m.Client.CreateTable(context.TODO(), input)

	if err != nil {
		log.Fatalf("an error ocurred while crating the table: %v", err)
		return err
	} else {
		waiter := dynamodb.NewTableExistsWaiter(m.Client)
		err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(m.Name)}, 5*time.Minute)
		if err != nil {
			log.Printf("Wait for table exists failed. Here's why: %v\n", err)
		}
	}

	return nil
}

func (m Table) ListTables() ([]string, error) {
	var tableNames []string
	tables, err := m.Client.ListTables(context.TODO(), &dynamodb.ListTablesInput{})
	if err != nil {
		log.Printf("an error ocurred while listing the tables: %v", err)
		return nil, err
	} else {
		tableNames = tables.TableNames
	}

	return tableNames, nil
}

func (m Table) DeleteTable() error {
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(m.Name),
	}

	_, err := m.Client.DeleteTable(context.TODO(), input)

	if err != nil {
		log.Println("error while deleting the selected table: ", err)
		return err
	}

	return nil
}

func (m Table) AddMovie(movie domain.Movie) error {
	item, err := attributevalue.MarshalMap(movie)

	if err != nil {
		log.Println("error marshaling the movie struct: ", err)
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(m.Name),
	}

	_, err = m.Client.PutItem(context.TODO(), input)

	if err != nil {
		log.Println("error putting the item on the table: ", err)
		return err
	}

	return nil
}

func (m Table) GetMovie(year int, title string) (domain.Movie, error) {
	movie := domain.Movie{Year: year, Title: title}
	input := &dynamodb.GetItemInput{
		TableName: aws.String(m.Name),
		Key:       movie.GetKey(),
	}

	response, err := m.Client.GetItem(context.TODO(), input)

	if err != nil {
		log.Println("error getting the item of the table: ", err)
		return domain.Movie{}, err
	} else {
		if err := attributevalue.UnmarshalMap(response.Item, &movie); err != nil {
			log.Println("error unmarshalling the movie struct: ", err)
		}
	}

	return movie, nil
}

func (m Table) AddMovieBatch(movies []domain.Movie, maxMovies int) (int, error) {
	written := 0
	batchSize := 25
	start := 0
	end := start + batchSize
	for start < maxMovies && start < len(movies) {
		var writeReqs []types.WriteRequest
		if end > len(movies) {
			end = len(movies)
		}
		for _, movie := range movies[start:end] {
			item, err := attributevalue.MarshalMap(movie)
			if err != nil {
				log.Println("error marshaling the movie struct: ", err)
				return 0, err
			} else {
				writeReqs = append(writeReqs, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
			}
		}
		_, err := m.Client.BatchWriteItem(context.TODO(), &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{m.Name: writeReqs},
		})

		if err != nil {
			log.Println("error writen the batch of movie structs: ", err)
			return 0, err
		} else {
			written += len(writeReqs)
		}
		start = end
		end += batchSize
	}

	return written, nil
}

/*
func (m Table) UpdateMovie(movie domain.Movie) error {
	updateExpression := expression.Set(expression.Name("info.rating"), expression.Value(movie.Info["rating"]))
	updateExpression.Set(expression.Name("info.plot"), expression.Value(movie.Info["plot"]))
}
*/

func (m Table) ScanMoviesByYear(startYear int, endYear int) ([]domain.Movie, error) {
	var movies []domain.Movie
	var err error
	var response *dynamodb.ScanOutput
	filter := expression.Name("year").Between(expression.Value(startYear), expression.Value(endYear))
	projections := expression.NamesList(expression.Name("year"), expression.Name("title"), expression.Name("info.rating"))

	expr, err := expression.NewBuilder().WithFilter(filter).WithProjection(projections).Build()

	if err != nil {
		log.Println("error building the sacnning expression: ", err)
		return nil, err
	} else {
		response, err = m.Client.Scan(context.TODO(), &dynamodb.ScanInput{
			TableName:                 aws.String(m.Name),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			FilterExpression:          expr.Filter(),
			ProjectionExpression:      expr.Projection(),
		})

		if err != nil {
			log.Println("error executing the scanning expresion. Here's why: ", err)
			return nil, err
		} else {
			if err := attributevalue.UnmarshalListOfMaps(response.Items, &movies); err != nil {
				log.Println("error unmarshalling the movies expresion. Here's why: ", err)
				return nil, err
			}
		}
	}

	return movies, nil
}

func (m Table) Scan() ([]domain.Movie, error) {
	var movies []domain.Movie

	response, err := m.Client.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName: aws.String(m.Name),
	})

	if err != nil {
		log.Println("error quetting the table items: ", err)
		return nil, err
	}

	if err := attributevalue.UnmarshalListOfMaps(response.Items, &movies); err != nil {
		log.Println("error unmarshalling the movies expresion. Here's why: ", err)
		return nil, err
	}

	return movies, nil
}

func (m Table) Query(releaseYear int) ([]domain.Movie, error) {
	var movies []domain.Movie
	queryExp := expression.Key("year").Equal(expression.Value(releaseYear))

	exp, err := expression.NewBuilder().WithKeyCondition(queryExp).Build()

	if err != nil {
		log.Println("error building the query expression: ", err)
		return nil, err
	} else {
		response, err := m.Client.Query(context.TODO(), &dynamodb.QueryInput{
			TableName:                 aws.String(m.Name),
			ExpressionAttributeNames:  exp.Names(),
			ExpressionAttributeValues: exp.Values(),
			KeyConditionExpression:    exp.KeyCondition(),
		})

		if err != nil {
			log.Println("error building the query expression: ", err)
			return nil, err
		} else {
			if err := attributevalue.UnmarshalListOfMaps(response.Items, &movies); err != nil {
				log.Println("error unmarshalling the movies expresion. Here's why: ", err)
				return nil, err
			}
		}
	}

	return movies, nil
}

func (m Table) DeleteMovie(year int, title string) error {
	movie := domain.Movie{Year: year, Title: title}
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(m.Name),
		Key:       movie.GetKey(),
	}

	_, err := m.Client.DeleteItem(context.TODO(), input)

	if err != nil {
		log.Println("error deleting the item of the table: ", err)
		return err
	}

	return nil
}
