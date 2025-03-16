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
	UpdatedAt time.Time `bson:"updated_at,omitempty" json:"updated_at,omitempty"`
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

func (us *UserStore) RegisterUser(ctx context.Context, user User) error {
	_, err := us.collection.InsertOne(ctx, user)
	if mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("user already exists")
	}

	return nil
}

func (us *UserStore) GetUserByUsername(ctx context.Context, username string) (*User, error) {
	var user User
	resp := us.collection.FindOne(ctx, bson.M{"username": username}).Decode(&user)
	if resp != nil {
		if resp == mongo.ErrNoDocuments {
			return nil, nil
		}

		return nil, resp
	}

	return &user, nil
}

func (us *UserStore) GetUserByUserID(ctx context.Context, id string) (*User, error) {
	var user User
	err := us.collection.FindOne(ctx, bson.M{"_id": id}).Decode(&user)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, err
		}
	}

	return &user, nil
}

func (us *UserStore) UpdateUserLastOnlineTime(ctx context.Context, username string) error {
	_, err := us.collection.UpdateOne(
		ctx,
		bson.M{"username": username},
		bson.M{"$set": bson.M{"last_seen": time.Now(), "updated_at": time.Now()}},
	)

	return err
}

func (us *UserStore) UpdateUsername(ctx context.Context, oldUsername, newUsername string) error {
	_, err := us.collection.UpdateOne(
		ctx,
		bson.M{"username": oldUsername},
		bson.M{"$set": bson.M{"username": newUsername, "updated_at": time.Now()}},
	)

	return err
}

func (us *UserStore) ListAllUsers(ctx context.Context, limit, skip int64) ([]User, error) {
	Opts := options.Find()
	Opts.SetLimit(limit)
	Opts.SetSkip(skip)

	cursor, err := us.collection.Find(ctx, bson.M{}, Opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var users []User
	if err := cursor.All(ctx, &users); err != nil {
		return nil, err
	}
	return users, nil
}

func (us *UserStore) DeleteUserByID(ctx context.Context, id string) (bool, error) {
	resp, err := us.collection.DeleteOne(ctx, bson.M{"_id": id})

	if err != nil {
		return false, err
	}

	if resp.DeletedCount == 0 {
		return false, nil
	}

	return true, nil
}

func (us *UserStore) CountUserDocument(ctx context.Context) (int64, error) {
	return us.collection.CountDocuments(ctx, bson.M{})
}
