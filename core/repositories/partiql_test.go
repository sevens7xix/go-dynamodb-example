package repositories

import (
	"errors"
	"fmt"
	"go-dynamodb-example/core/domain"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/awsdocs/aws-doc-sdk-examples/gov2/dynamodb/stubs"
	"github.com/awsdocs/aws-doc-sdk-examples/gov2/testtools"
)

func InitStubs() (*testtools.AwsmStubber, *PartiQLRunner) {
	stubber := testtools.NewStubber()
	runner := &PartiQLRunner{Client: dynamodb.NewFromConfig(*stubber.SdkConfig), TableName: "test-table"}
	return stubber, runner
}

func TestAddMovieWithPartiQL(t *testing.T) {
	tests := []struct {
		name string
		err  *testtools.StubError
	}{
		{"NoErrors", nil},
		{"TestError", &testtools.StubError{Err: errors.New("TestError")}},
	}

	for _, test := range tests {
		t.Run(
			test.name, func(t *testing.T) {
				AddMovieWithPartiQLTest(test.err, t)
			},
		)
	}
}

func TestUpdateMovieWithPartiQL(t *testing.T) {
	tests := []struct {
		name string
		err  *testtools.StubError
	}{
		{"NoErrors", nil},
		{"TestError", &testtools.StubError{Err: errors.New("TestError")}},
	}

	for _, test := range tests {
		t.Run(
			test.name, func(t *testing.T) {
				UpdateMovieWithPartiQLTest(test.err, t)
			},
		)
	}
}

func TestGetMovieWithPartiQL(t *testing.T) {
	tests := []struct {
		name string
		err  *testtools.StubError
	}{
		{"NoErrors", nil},
		{"TestError", &testtools.StubError{Err: errors.New("TestErrorGetMovie")}},
	}

	for _, test := range tests {
		t.Run(
			test.name, func(t *testing.T) {
				GetMovieWithPartiQLTest(test.err, t)
			},
		)
	}
}

func TestDeleteMovieWithPartiQL(t *testing.T) {
	tests := []struct {
		name string
		err  *testtools.StubError
	}{
		{"NoErrors", nil},
		{"TestError", &testtools.StubError{Err: errors.New("TestError")}},
	}

	for _, test := range tests {
		t.Run(
			test.name, func(t *testing.T) {
				DeleteMovieWithPartiQLTest(test.err, t)
			},
		)
	}
}

func AddMovieWithPartiQLTest(raiseErr *testtools.StubError, t *testing.T) {
	stubber, runner := InitStubs()

	movie := domain.Movie{Title: "Test movie", Year: 2001, Info: map[string]interface{}{
		"rating": 3.5, "plot": "Not bad."}}

	stubber.Add(stubs.StubExecuteStatement(
		fmt.Sprintf("INSERT INTO \"%v\" VALUE {'title': ?, 'year': ?, 'info': ?}", runner.TableName),
		[]interface{}{movie.Title, movie.Year, movie.Info}, nil, raiseErr))

	err := runner.AddMovieWithPartiQL(movie)

	testtools.VerifyError(err, raiseErr, t)
	testtools.ExitTest(stubber, t)
}

func GetMovieWithPartiQLTest(raiseErr *testtools.StubError, t *testing.T) {
	stubber, runner := InitStubs()

	movie := domain.Movie{Title: "Test movie", Year: 2001, Info: map[string]interface{}{"rating": 3.5, "plot": "Not bad."}}

	stubber.Add(stubs.StubExecuteStatement(fmt.Sprintf("SELECT * FROM \"%v\" WHERE title = ? AND year = ?", runner.TableName), []interface{}{movie.Title, movie.Year}, movie, raiseErr))

	actual, err := runner.GetMovieWithPartiQL(movie.Title, movie.Year)

	if err == nil {
		if actual.Info["rating"] != movie.Info["rating"] || actual.Info["plot"] != movie.Info["plot"] {
			t.Errorf("actual: %s, expected: %s", actual.String(), movie.String())
		}
	}
	testtools.VerifyError(err, raiseErr, t)
	testtools.ExitTest(stubber, t)

}

func UpdateMovieWithPartiQLTest(raiseErr *testtools.StubError, t *testing.T) {
	stubber, runner := InitStubs()

	movie := domain.Movie{Title: "Test movie", Year: 2001, Info: map[string]interface{}{"rating": 3.5, "plot": "Not bad."}}
	rating := 4.5

	stubber.Add(stubs.StubExecuteStatement(fmt.Sprintf("UPDATE \"%v\" SET info.rating = ? WHERE title = ? AND year = ?", runner.TableName), []interface{}{rating, movie.Title, movie.Year}, nil, raiseErr))

	err := runner.UpdateMovieWithPartiQL(movie, rating)

	testtools.VerifyError(err, raiseErr, t)
	testtools.ExitTest(stubber, t)
}

func DeleteMovieWithPartiQLTest(raiseErr *testtools.StubError, t *testing.T) {
	stubber, runner := InitStubs()

	title := "Test Movie"
	year := 2023

	stubber.Add(stubs.StubExecuteStatement(fmt.Sprintf("DELETE FROM \"%v\" WHERE title = ? AND year = ?", runner.TableName), []interface{}{title, year}, nil, raiseErr))

	err := runner.DeleteMovieWithPartiQL(title, year)

	testtools.VerifyError(err, raiseErr, t)
	testtools.ExitTest(stubber, t)
}
