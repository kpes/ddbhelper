package ddbhelper

const (
	pk = "pk"
)

type tableDefinition struct {
	tableName        string
	tableIndex       index
	secondaryIndexes map[string]index
}

func CreateTableDefinition(tableName string, options ...func(*tableDefinition)) tableDefinition {
	if len(tableName) == 0 {
		panic("table name cannot be empty")
	}
	definition := tableDefinition{tableName: tableName, secondaryIndexes: map[string]index{}}

	for _, option := range options {
		option(&definition)
	}

	return definition
}

// Option functions
func WithTableIndex(index index) func(*tableDefinition) {
	return func(definition *tableDefinition) {
		definition.tableIndex = index
	}
}

func WithSecondaryIndex(name string, index index) func(*tableDefinition) {
	if len(name) == 0 {
		panic("index name cannot be empty")
	}
	return func(definition *tableDefinition) {
		definition.secondaryIndexes[name] = index
	}
}

type index struct {
	partitionKeyAttribute string
	sortKeyAttribute      string
}

func CreateIndex(partitionKeyAttribute string, sortKeyAttribute string) index {
	if len(partitionKeyAttribute) == 0 {
		panic("partition key attribute cannot be empty")
	}
	return index{partitionKeyAttribute: partitionKeyAttribute, sortKeyAttribute: sortKeyAttribute}
}

func (s index) getPartitionKeyAttribute() string {
	if len(s.partitionKeyAttribute) > 0 {
		return s.partitionKeyAttribute
	}
	return pk
}

func (s index) getSortKeyAttribute() string {
	if len(s.sortKeyAttribute) > 0 {
		return s.sortKeyAttribute
	}
	return ""
}

func (s index) hasSortKey() bool {
	return len(s.sortKeyAttribute) > 0
}
