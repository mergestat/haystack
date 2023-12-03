package pile_test

import (
	"context"
	"testing"

	"github.com/mergestat/haystack/pkg/pile"
)

func TestAddSingleRepo(t *testing.T) {
	const fixtureRepo = "https://github.com/mergestat/mergestat"
	p, err := pile.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	ctx := context.Background()
	if err = p.AddRepo(ctx, fixtureRepo); err != nil {
		t.Fatal(err)
	}

	repos, err := p.ListRepos(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(repos) != 1 {
		t.Fatalf("expected 1 repo, got %d", len(repos))
	}

	if repos[0] != fixtureRepo {
		t.Fatalf("expected %s, got %s", fixtureRepo, repos[0])
	}
}
