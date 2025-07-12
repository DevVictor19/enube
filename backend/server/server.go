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
	"github.com/DevVictor19/enube/backend/server/services"
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

	cfg, err := env.GetEnv()
	if err != nil {
		panic(err)
	}

	repos := repositories.NewRepositories(database, db.QueryDuration)
	svcs := services.NewServices(cfg)
	ctls := controllers.NewControllers(repos, svcs)

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

		r.Route("/auth", func(r chi.Router) {
			r.Post("/login", ctls.Auth.Login)
		})

		// protected routes
		r.Group(func(r chi.Router) {
			r.Use(makeJWTAuthMiddleware(svcs.JWT, repos.User))

			r.Route("/charges", func(r chi.Router) {
				r.Get("/", ctls.Charge.FindPaginated)
				r.Get("/resume", ctls.Charge.GetResume)
			})

			r.Get("/customers", ctls.Customer.FindPaginated)
			r.Get("/partners", ctls.Partner.FindPaginated)
			r.Get("/products", ctls.Product.FindPaginated)
			r.Get("/months_charge_dates", ctls.MonthChargeDate.FindPaginated)
			r.Get("/usage_dates", ctls.UsageDate.FindPaginated)
			r.Get("/billing_currencies", ctls.BillingCurrency.FindPaginated)
			r.Get("/pricing_currencies", ctls.PricingCurrency.FindPaginated)
			r.Get("/resource_locations", ctls.ResourceLocation.FindPaginated)
			r.Get("/services", ctls.Service.FindPaginated)
		})
	})
	return r
}
