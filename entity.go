package ddbhelper

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type entityHandler[T record] struct {
	table     tableDefinition
	ddbClient ddbClient
}

func CreateEntityHandler[T record](table tableDefinition, ddbClient *dynamodb.Client) entityHandler[T] {
	if ddbClient == nil {
		panic("dynamodb client cannot be nil")
	}
	return entityHandler[T]{table: table, ddbClient: ddbClient}
}

func (s entityHandler[T]) createKey(t T) map[string]types.AttributeValue {
	if !s.table.tableIndex.hasSortKey() && t.SortKey() != "" {
		panic(fmt.Errorf("sort key attribute is not defined for table %s", s.table.tableName))
	}

	key := map[string]types.AttributeValue{}
	key[s.table.tableIndex.getPartitionKeyAttribute()] = &types.AttributeValueMemberS{Value: t.PartitionKey()}
	if s.table.tableIndex.hasSortKey() {
		key[s.table.tableIndex.getSortKeyAttribute()] = &types.AttributeValueMemberS{Value: t.SortKey()}
	}
	return key
}

func (s entityHandler[T]) Insert(ctx context.Context, t T) error {
	av, err := attributevalue.MarshalMap(t)
	if err != nil {
		return fmt.Errorf("failed to marshal attribute value: %w", err)
	}

	key := s.createKey(t)
	av[s.table.tableIndex.getPartitionKeyAttribute()] = key[s.table.tableIndex.getPartitionKeyAttribute()]
	if s.table.tableIndex.hasSortKey() {
		av[s.table.tableIndex.getSortKeyAttribute()] = key[s.table.tableIndex.getSortKeyAttribute()]
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(s.table.tableName),
		Item:      av,
	}

	if _, err = s.ddbClient.PutItem(ctx, input); err != nil {
		return fmt.Errorf("failed to put item: %w", err)
	}

	return nil
}

func (s entityHandler[T]) Delete(ctx context.Context, t T) error {
	key := s.createKey(t)

	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(s.table.tableName),
		Key:       key,
	}

	if _, err := s.ddbClient.DeleteItem(ctx, input); err != nil {
		return fmt.Errorf("failed to delete item: %w", err)
	}

	return nil
}

func (s entityHandler[T]) Update(ctx context.Context, t T, attributes map[string]interface{}) error {
	key := s.createKey(t)

	updateExpression := "SET "
	expressionAttributeValues := make(map[string]types.AttributeValue)

	for attrName, attrValue := range attributes {
		updateExpression += fmt.Sprintf("%s = :%s, ", attrName, attrName)
		attrVal, err := attributevalue.Marshal(attrValue)
		if err != nil {
			return fmt.Errorf("failed to marshal attribute value: %w", err)
		}
		expressionAttributeValues[fmt.Sprintf(":%s", attrName)] = attrVal
	}

	updateExpression = strings.TrimSuffix(updateExpression, ", ")

	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(s.table.tableName),
		Key:                       key,
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeValues: expressionAttributeValues,
	}

	if _, err := s.ddbClient.UpdateItem(ctx, input); err != nil {
		return fmt.Errorf("failed to update item: %w", err)
	}

	return nil
}

func (s entityHandler[T]) Get(ctx context.Context, t T) (*T, error) {
	key := s.createKey(t)

	input := &dynamodb.GetItemInput{
		TableName: aws.String(s.table.tableName),
		Key:       key,
	}

	result, err := s.ddbClient.GetItem(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to get item: %w", err)
	}

	if result.Item == nil {
		return nil, nil
	}

	item := new(T)
	if err := attributevalue.UnmarshalMap(result.Item, item); err != nil {
		return nil, fmt.Errorf("failed to unmarshal item: %w", err)
	}

	return item, nil
}
