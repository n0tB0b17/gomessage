package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/n0tB0b17/gomessage/internal/models"
)

func (a *APIServer) RegisterUser(w http.ResponseWriter, r *http.Request) {
	var resp struct {
		Username string `json:"username"`
	}

	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		http.Error(w, fmt.Sprintf("invalid body provided"), http.StatusBadRequest)
		return
	}

	if resp.Username == "" {
		http.Error(w, fmt.Sprintf("empty username provided"), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	usr, err := a.userStore.GetUserByUsername(ctx, resp.Username)
	if err != nil {
		http.Error(w, fmt.Sprintf("error checking username: %s ", resp.Username), http.StatusInternalServerError)
		return
	}

	if usr != nil {
		http.Error(w, fmt.Sprintf("user already exists with username: %s", resp.Username), http.StatusConflict)
		return
	}

	user := models.User{
		ID:        uuid.New().String(),
		Username:  resp.Username,
		CreatedAt: time.Now(),
		LastSeen:  time.Now(),
	}

	if err := a.userStore.RegisterUser(ctx, user); err != nil {
		http.Error(w, fmt.Sprintf("error while registering user"), http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(user)
}

func (a *APIServer) FetchRegisteredUsers(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	limit := int64(100)
	skip := int64(0)
	users, err := a.userStore.ListAllUsers(ctx, limit, skip)
	if err != nil {
		http.Error(w, fmt.Sprintf("error while fetching registered users from database"), http.StatusInternalServerError)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(users)
}
