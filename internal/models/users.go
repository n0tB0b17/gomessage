package models

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type User struct {
	ID        string    `bson:"_id" json:"id"`
	Username  string    `bson:"username" json:"username"`
	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	LastSeen  time.Time `bson:"last_seen" json:"last_seen"`
}

type UserStore struct {
	collection *mongo.Collection
}

func NewUserStore(client *mongo.Client, dbName string) *UserStore {
	col := client.Database(dbName).Collection("users")
	idxModel := mongo.IndexModel{
		Keys:    bson.D{{Key: "username", Value: 1}},
		Options: options.Index().SetUnique(true),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := col.Indexes().CreateOne(ctx, idxModel)
	if err != nil {
		fmt.Printf("error while creating users collection: %v \n", err)
	}
	return &UserStore{
		collection: col,
	}
}
