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

type EntityHandler[T Record] struct {
	TableDef  TableDefinition
	DdbClient ddbClient
}

func (s EntityHandler[T]) createKey(t T) map[string]types.AttributeValue {
	if !s.TableDef.TableIndex.hasSortKey() && t.SortKey() != "" {
		panic(fmt.Errorf("sort key attribute is not defined for table %s", s.TableDef.TableName))
	}

	key := map[string]types.AttributeValue{}
	key[s.TableDef.TableIndex.getPartitionKeyAttribute()] = &types.AttributeValueMemberS{Value: t.PartitionKey()}
	if s.TableDef.TableIndex.hasSortKey() {
		key[s.TableDef.TableIndex.getSortKeyAttribute()] = &types.AttributeValueMemberS{Value: t.SortKey()}
	}
	return key
}

func (s EntityHandler[T]) Insert(ctx context.Context, t T) error {
	av, err := attributevalue.MarshalMap(t)
	if err != nil {
		return fmt.Errorf("failed to marshal attribute value: %w", err)
	}

	key := s.createKey(t)
	av[s.TableDef.TableIndex.getPartitionKeyAttribute()] = key[s.TableDef.TableIndex.getPartitionKeyAttribute()]
	if s.TableDef.TableIndex.hasSortKey() {
		av[s.TableDef.TableIndex.getSortKeyAttribute()] = key[s.TableDef.TableIndex.getSortKeyAttribute()]
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(s.TableDef.TableName),
		Item:      av,
	}

	if _, err = s.DdbClient.PutItem(ctx, input); err != nil {
		return fmt.Errorf("failed to put item: %w", err)
	}

	return nil
}

func (s EntityHandler[T]) Delete(ctx context.Context, t T) error {
	key := s.createKey(t)

	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(s.TableDef.TableName),
		Key:       key,
	}

	if _, err := s.DdbClient.DeleteItem(ctx, input); err != nil {
		return fmt.Errorf("failed to delete item: %w", err)
	}

	return nil
}

func (s EntityHandler[T]) Update(ctx context.Context, t T, attributes map[string]interface{}) error {
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
		TableName:                 aws.String(s.TableDef.TableName),
		Key:                       key,
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeValues: expressionAttributeValues,
	}

	if _, err := s.DdbClient.UpdateItem(ctx, input); err != nil {
		return fmt.Errorf("failed to update item: %w", err)
	}

	return nil
}

func (s EntityHandler[T]) Get(ctx context.Context, t T) (*T, error) {
	key := s.createKey(t)

	input := &dynamodb.GetItemInput{
		TableName: aws.String(s.TableDef.TableName),
		Key:       key,
	}

	result, err := s.DdbClient.GetItem(ctx, input)
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
