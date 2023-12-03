package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/mergestat/haystack/pkg/pile"
)

var (
	connection string
)

func init() {
	flag.StringVar(&connection, "connection", "", "sqlite connection string")
}

func handleErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	flag.Parse()

	p, err := pile.Open(pile.WithConnection(connection))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	ctx := context.Background()

	switch flag.Arg(0) {
	case "add-repo":
		repoURL := flag.Arg(1)
		err := p.AddRepo(ctx, repoURL)
		handleErr(err)
	case "list-repos":
		repos, err := p.ListRepos(ctx)
		handleErr(err)
		for _, repo := range repos {
			fmt.Println(repo)
		}
	case "search-repos":
		query := flag.Arg(1)
		results, err := p.SearchAllRepoContents(ctx, query)
		handleErr(err)
		for _, result := range results {
			fmt.Println(result)
		}
	default:
		fmt.Println("unknown command")
	}
}
