package main

import (
	"fmt"
	"go-dynamodb-example/core/repositories"
)

func main() {
	runner := repositories.NewPartiQLRunner("movies")

	queryMovie, _ := runner.GetMovieWithPartiQL("Super Mario Bros", 2023)

	fmt.Println(queryMovie.String())

	/*

		movie := domain.Movie{
			Year:  2023,
			Title: "Super Mario Bros",
			Info: map[string]interface{}{
				"plot":   "Movie based on the Super Mario Bros game of Nintendo",
				"rating": 2.5,
			},
		}
				movies := []domain.Movie{{
					Year:  1988,
					Title: "Akira",
					Info: map[string]interface{}{
						"plot":   "cool kids riding cool bikes turned pk",
						"rating": 5,
					}},
					{
						Year:  2023,
						Title: "Barbie",
						Info: map[string]interface{}{
							"plot":   "She's barbie",
							"rating": 3,
						}},
					{
						Year:  1986,
						Title: "Star Wars",
						Info: map[string]interface{}{
							"plot":   "Geoge Lucas magnum opus",
							"rating": 5,
						}},
					{
						Year:  1983,
						Title: "Scarface",
						Info: map[string]interface{}{
							"plot":   "Say hello to my little friend",
							"rating": 3,
						}},
					{
						Year:  2021,
						Title: "Suicide Squad (2021)",
						Info: map[string]interface{}{
							"plot":   "James Gunn redemeed this saga",
							"rating": 4,
						}},
				}

				records, err := client.AddMovieBatch(movies, len(movies))

				if err != nil {
					fmt.Println(err)
				}

				fmt.Println("Records written: ", records)


			fetchRecord, err := client.Scan()

			if err != nil {
				fmt.Println(err)
			}

			for _, record := range fetchRecord {
				fmt.Println(record.String())
			}
	*/

}
