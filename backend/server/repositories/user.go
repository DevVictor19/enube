package repositories

import (
	"context"
	"database/sql"
	"time"
)

type UserRepository struct {
	db           *sql.DB
	queryTimeout time.Duration
}

func NewUserRepository(db *sql.DB, qt time.Duration) *UserRepository {
	return &UserRepository{
		db:           db,
		queryTimeout: qt,
	}
}

func (r *UserRepository) FindByEmail(ctx context.Context, email string) (*User, error) {
	const query = `
		SELECT id, email, password
		FROM users
		WHERE email = $1
		LIMIT 1;
	`

	var user User
	err := r.db.QueryRowContext(ctx, query, email).Scan(
		&user.ID,
		&user.Email,
		&user.Password,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return &user, nil
}

func (r *UserRepository) FindByID(ctx context.Context, id int) (*User, error) {
	const query = `
		SELECT id, email, password
		FROM users
		WHERE id = $1
		LIMIT 1;
	`

	var user User
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&user.ID,
		&user.Email,
		&user.Password,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return &user, nil
}
