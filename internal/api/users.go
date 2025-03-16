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
		return
	}

	count, err := a.userStore.CountUserDocument(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("unable to fetch total count of users"), http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	resp := map[string]interface{}{
		"count": count,
		"users": users,
	}

	json.NewEncoder(w).Encode(resp)
}

func (a *APIServer) UpdateUsername(w http.ResponseWriter, r *http.Request) {
	var resp struct {
		Username string `json:"username"`
	}

	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, fmt.Sprintf("empty user id provided"), http.StatusBadRequest)
		return
	}

	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		http.Error(w, fmt.Sprintf("invalid body provided"), http.StatusBadRequest)
		return
	}

	if resp.Username == "" {
		http.Error(w, fmt.Sprintf("username cannot be empty"), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	user, err := a.userStore.GetUserByUserID(ctx, id)
	if err != nil {
		http.Error(w, fmt.Sprintf("unable to find user with given id, update declined"), http.StatusInternalServerError)
		return
	}

	err = a.userStore.UpdateUsername(ctx, user.Username, resp.Username)
	if err != nil {
		http.Error(w, fmt.Sprintf("unable to update user: %+v", user), http.StatusInternalServerError)
		return
	}

	newResp := map[string]interface{}{
		"old_username": user.Username,
		"new_username": resp.Username,
		"message":      "the username has been updated successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(newResp)
}

func (a *APIServer) DeleteUser(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, fmt.Sprintf("invalid user id provided"), http.StatusConflict)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// check if user exist in our database
	user, err := a.userStore.GetUserByUserID(ctx, id)
	if err != nil {
		http.Error(w, fmt.Sprintf("Database response error."), http.StatusInternalServerError)
		return
	}

	if user == nil {
		http.Error(w, fmt.Sprintf("User with given id: %s not found", id), http.StatusConflict)
		return
	}

	// delete userID from database
	resp, err := a.userStore.DeleteUserByID(ctx, id)
	if err != nil && !resp {
		http.Error(w, fmt.Sprintf("error while deleting user"), http.StatusInternalServerError)
		return
	}

	newResp := map[string]interface{}{
		"isDeleted":   true,
		"userDeleted": resp,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusFound)
	json.NewEncoder(w).Encode(newResp)
}
