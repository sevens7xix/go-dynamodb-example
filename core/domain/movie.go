package domain

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Movie struct {
	Title string                 `dynamodbav:"title"`
	Year  int                    `dynamodbav:"year"`
	Info  map[string]interface{} `dynamodbav:"info"`
}

func (movie Movie) GetKey() map[string]types.AttributeValue {
	title, err := attributevalue.Marshal(movie.Title)
	if err != nil {
		log.Fatalf("error marshaling the title attribute: %v", err)
	}
	year, err := attributevalue.Marshal(movie.Year)
	if err != nil {
		log.Fatalf("error marshaling the year attribute: %v", err)
	}

	return map[string]types.AttributeValue{"year": year, "title": title}
}

func (movie Movie) String() string {
	return fmt.Sprintf("%v\n\tReleased: %v\n\tRating: %v\n\tPlot: %v\n",
		movie.Title, movie.Year, movie.Info["rating"], movie.Info["plot"])
}
