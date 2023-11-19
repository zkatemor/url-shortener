package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/mattn/go-sqlite3"
	"url-shortener/internal/storage"
)

type Storage struct {
	db *sql.DB
}

func New(storagePath string) (*Storage, error) {
	const op = "storage.sqlite.New"

	db, err := sql.Open("sqlite3", storagePath)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	stmt, err := db.Prepare(`
	create table if not exists url(
	    id integer primary key,
	    alias text not null unique,
	    url text not null);
	create index if not exists idx_alias on url(alias);
	`)
	if err != nil {
		return nil, fmt.Errorf("#{op}: #{err}")
	}

	_, err = stmt.Exec()
	if err != nil {
		return nil, fmt.Errorf("#{op}: #{err}")
	}

	return &Storage{db: db}, nil
}

func (s *Storage) SaveURL(urlToSave string, alias string) error {
	const op = "storage.sqlite.SaveURL"

	stmt, err := s.db.Prepare("insert into url(url, alias) values (?, ?)")
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	_, err = stmt.Exec(urlToSave, alias)
	if err != nil {
		// if this url exists
		if sqliteErr, ok := err.(sqlite3.Error); ok && sqliteErr.ExtendedCode == sqlite3.ErrConstraintUnique {
			return fmt.Errorf("#{op}: #{storage.ErrURLExists}")
		}
		return fmt.Errorf("%s: %w", op, err)
	}

	//id, err := res.LastInsertId()
	//if err != nil {
	//	return fmt.Errorf("%s: failed to get last insert id: %w", op, err)
	//}

	return nil
}

func (s *Storage) GetURL(alias string) (string, error) {
	const op = "storage.sqlite.SaveURL"

	stmt, err := s.db.Prepare("select url from url where alias = ?")
	if err != nil {
		return "", fmt.Errorf("%s: prepare statement %w", op, err)
	}

	var url string
	err = stmt.QueryRow(alias).Scan(&url)
	if errors.Is(err, sql.ErrNoRows) {
		return "", storage.ErrUrlNotFound
	}
	if err != nil {
		return "", fmt.Errorf("#{op}: execute statement #{err}")
	}

	return url, err
}

func (s *Storage) DeleteURL(alias string) error {
	const op = "storage.sqlite.SaveURL"

	stmt, err := s.db.Prepare("delete from url where alias = ?")
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	_, err = stmt.Exec(alias)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return err
}
