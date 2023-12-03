package pile

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"runtime"

	"github.com/mergestat/gitutils/clone"
	"github.com/mergestat/gitutils/gitlog"
	"github.com/mergestat/gitutils/lsfiles"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type Pile struct {
	pool      *sqlitex.Pool
	clonePath string
}

type openOption func(*Pile) error

// WithConnection sets the connection string for the pile. If this is not set, the pile will be in-memory only.
func WithConnection(connection string) openOption {
	return func(p *Pile) error {
		// TODO(patrickdevivo) not sure if this is a sensible default pool size...investigate this.
		pool, err := sqlitex.Open(connection, 0, runtime.NumCPU())
		if err != nil {
			return err
		}
		p.pool = pool

		return nil
	}
}

// WithRepoClonePath sets the path where repos will be cloned to on disk.
func WithRepoClonePath(filePath string) openOption {
	return func(p *Pile) error {
		if err := os.MkdirAll(filePath, 0755); err != nil {
			return err
		}

		p.clonePath = filePath
		return nil
	}
}
func Open(opts ...openOption) (*Pile, error) {
	p := &Pile{}

	for _, opt := range opts {
		if err := opt(p); err != nil {
			return nil, err
		}
	}

	// if the user didn't set a connection string, set a default one (in memory db)
	if p.pool == nil {
		err := WithConnection("file::memory:?cache=shared")(p)
		if err != nil {
			return nil, err
		}
	}

	// if the user didn't set a repo clone path, set a default one to a new tmp dir
	if p.clonePath == "" {
		var err error
		if p.clonePath, err = os.MkdirTemp("", "mergestat-haystack-*"); err != nil {
			return nil, err
		}
	}

	// ensure the schema is present on open
	if err := p.ensureSchema(context.TODO()); err != nil {
		return nil, err
	}

	return p, nil
}

const schema = `
CREATE TABLE IF NOT EXISTS repos (
	id INTEGER PRIMARY KEY,
	url TEXT UNIQUE,
	last_indexed_commit_hash TEXT
);

CREATE TABLE IF NOT EXISTS repo_contents (
	repo_id INTEGER,
	path TEXT,
	content TEXT
);
`

// ensureSchema ensures the schema of the pile is up to date.
func (p *Pile) ensureSchema(ctx context.Context) error {
	conn := p.pool.Get(ctx)
	if conn == nil {
		return errors.New("could not get connection from pool")
	}
	defer p.pool.Put(conn)

	if err := sqlitex.ExecuteScript(conn, schema, nil); err != nil {
		return err
	}

	return nil
}

// AddRepo clones a repo and adds it to the pile.
func (p *Pile) AddRepo(ctx context.Context, repoURL string) error {

	// TODO(patrickdevivo) we should probably standardize the URL format for repos to consistently
	// handle things like trailing slashes, http vs https, etc.

	// TODO(patrickdevivo) if the repo already exists, check what the last indexed commit was and
	// only clone the new commits

	// first create a tmp dir to clone the repo into
	// using the p.clonePath as the parent directory
	cloneDir, err := os.MkdirTemp(p.clonePath, "mergestat-haystack-*")
	if err != nil {
		return err
	}

	// execute the clone
	if err := clone.Exec(ctx, repoURL, cloneDir, clone.WithDepth(1)); err != nil {
		return err
	}

	var latestCommitHash string
	commitIter, err := gitlog.Exec(ctx, cloneDir, gitlog.WithMaxCount(1))
	if err != nil {
		return err
	}

	c, err := commitIter.Next()
	if err != nil {
		return err
	}
	latestCommitHash = c.SHA

	conn := p.pool.Get(ctx)
	if conn == nil {
		return errors.New("could not get connection from pool")
	}
	defer p.pool.Put(conn)

	var txErr error
	end := sqlitex.Transaction(conn)
	defer end(&txErr)

	var lastIndexedCommitHash string
	sqlitex.Execute(conn, "SELECT url, last_indexed_commit_hash FROM repos WHERE url = ? LIMIT 1", &sqlitex.ExecOptions{
		Args: []interface{}{repoURL},
		ResultFunc: func(stmt *sqlite.Stmt) error {
			lastIndexedCommitHash = stmt.ColumnText(1)
			return nil
		},
	})

	// if the last indexed commit hash is the same as the latest commit hash, we don't need to do anything
	// so exit early
	if lastIndexedCommitHash == latestCommitHash {
		return nil
	}

	// insert the repo into the repos table if it doesn't already exist
	sqlitex.ExecuteTransient(conn, "INSERT INTO repos (url, last_indexed_commit_hash) VALUES (?, ?) ON CONFLICT (url) DO NOTHING", &sqlitex.ExecOptions{
		Args: []interface{}{repoURL, latestCommitHash},
	})

	fileIter, err := lsfiles.Exec(ctx, cloneDir)
	if err != nil {
		return err
	}

	// iterate over the files and add them to the pile
	// but exit early if the context is done
Exit:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			filePath, err := fileIter.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break Exit
				}
				return err
			}

			fullFilePath := path.Join(cloneDir, filePath)

			// TODO(patrickdevivo) this is probably not the most-memory efficient way of accessing the file contents.
			fileContents, err := os.ReadFile(fullFilePath)
			if err != nil {
				return err
			}

			// insert the file into the repo_contents table
			err = sqlitex.ExecuteTransient(conn, "INSERT INTO repo_contents (repo_id, path, content) VALUES ((SELECT id FROM repos WHERE url = ?), ?, ?)", &sqlitex.ExecOptions{
				Args: []interface{}{repoURL, filePath, fileContents},
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *Pile) ListRepos(ctx context.Context) ([]string, error) {
	conn := p.pool.Get(ctx)
	if conn == nil {
		return nil, errors.New("could not get connection from pool")
	}
	defer p.pool.Put(conn)

	var repos []string
	err := sqlitex.ExecuteTransient(conn, "SELECT url FROM repos", &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			repos = append(repos, stmt.ColumnText(0))
			return nil
		},
	})
	if err != nil {
		return nil, err
	}

	return repos, nil
}

func (p *Pile) SearchAllRepoContents(ctx context.Context, query string) ([]string, error) {
	conn := p.pool.Get(ctx)
	if conn == nil {
		return nil, errors.New("could not get connection from pool")
	}
	defer p.pool.Put(conn)

	var results []string
	// TODO(patrickdevivo) this query needs some improvement - not sure why GLOB and LIKE were not working here
	// maybe this is where something like FTS could make sense
	err := sqlitex.ExecuteTransient(conn, `SELECT path, repo_id FROM repo_contents WHERE instr(lower(content), lower(?)) > 0`, &sqlitex.ExecOptions{
		Args: []interface{}{query},
		ResultFunc: func(stmt *sqlite.Stmt) error {
			results = append(results, stmt.ColumnText(0))
			return nil
		},
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

func (p *Pile) Close() error {
	return p.pool.Close()
}
