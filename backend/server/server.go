package server

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/DevVictor19/enube/backend/server/controllers"
	"github.com/DevVictor19/enube/backend/server/db"
	"github.com/DevVictor19/enube/backend/server/env"
	"github.com/DevVictor19/enube/backend/server/repositories"
	"github.com/DevVictor19/enube/backend/server/utils"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func Start() error {
	cfg, err := env.LoadEnv()
	if err != nil {
		return err
	}

	database, err := db.Connect(
		cfg.DB.URL,
		cfg.DB.MaxOpenConns,
		cfg.DB.MaxIdleConns,
		cfg.DB.MaxIdleTime,
	)
	if err != nil {
		return err
	}
	defer database.Close()

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.ServerPort),
		Handler:      mount(),
		WriteTimeout: time.Second * 30,
		ReadTimeout:  time.Second * 10,
		IdleTimeout:  time.Minute,
	}

	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

func mount() http.Handler {
	database, err := db.Get()
	if err != nil {
		panic(err)
	}

	chargeRepo := repositories.NewChargeRepository(database, db.QueryDuration)
	chargeCtl := controllers.NewChargeController(chargeRepo)

	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))

	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
			utils.WriteJSON(w, http.StatusOK, map[string]string{"message": "ok"})
		})

		r.Route("/charges", func(r chi.Router) {
			r.Get("/", chargeCtl.FindPaginated)
		})
	})

	return r
}
