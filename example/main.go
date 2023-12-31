package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/kpes/ddbhelper"
)

const (
	pk = "pk"
	sk = "sk"

	gsi1   = "gsi1"
	gsi1pk = "gsi1pk"
	gsi1sk = "gsi1sk"

	tableName = "testtable"
)

type user struct {
	Id        string `dynamodbav:"id"`
	FirstName string `dynamodbav:"firstName"`
	LastName  string `dynamodbav:"lastName"`
	Email     string `dynamodbav:"email"`
}

func (u user) PartitionKey() string {
	return fmt.Sprintf("user:%s", u.Id)
}

func (u user) SortKey() string {
	return "meta"
}

func createDynamoDBClient() (*dynamodb.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithSharedConfigProfile("default"), config.WithRegion("us-east-2"))
	if err != nil {
		return nil, err
	}

	client := dynamodb.NewFromConfig(cfg)
	return client, nil
}

func main() {
	ctx := context.TODO()

	// Create DynamoDB client
	client, err := createDynamoDBClient()
	if err != nil {
		panic(err)
	}

	err = createTable(ctx, client)
	if err != nil {
		panic(err)
	}

	td := ddbhelper.CreateTableDefinition(tableName,
		ddbhelper.WithTableIndex(ddbhelper.CreateIndex(pk, sk)),
		ddbhelper.WithSecondaryIndex(gsi1, ddbhelper.CreateIndex(gsi1pk, gsi1sk)),
	)

	userId := uuid.NewString()

	userHandler := ddbhelper.CreateEntityHandler[user](td, client)
	fmt.Println("Inserting user...")
	if err := userHandler.Insert(context.TODO(), user{
		Id:        userId,
		FirstName: "John",
		LastName:  "Doe",
		Email:     "something@something.com",
	}); err != nil {
		panic(err)
	}

	fmt.Println("Getting user...")
	user, err := userHandler.Get(ctx, user{Id: userId})
	if err != nil {
		panic(err)
	}
	fmt.Printf("User: %+v\n", user)

	deleteTable(ctx, client)
}

func deleteTable(ctx context.Context, client *dynamodb.Client) error {
	fmt.Println("Deleting table...")
	_, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})
	return err
}

func createTable(ctx context.Context, client *dynamodb.Client) error {
	fmt.Println("Creating table...")
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(tableName),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(pk),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(sk),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(gsi1pk),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(gsi1sk),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(pk),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(sk),
				KeyType:       types.KeyTypeRange,
			},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(gsi1),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(gsi1pk),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(gsi1sk),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
	})
	if err != nil {
		return err
	}
	fmt.Print("Waiting for table to be created...")
	for {
		time.Sleep(3 * time.Second)
		out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			return err
		}
		if out.Table.TableStatus == types.TableStatusActive {
			break
		}
		fmt.Print(".")
	}
	fmt.Print("\n")
	return nil
}
