package ddbhelper

const (
	pk = "pk"
)

type TableDefinition struct {
	TableName        string
	TableIndex       Index
	SecondaryIndexes map[string]Index
}

type Index struct {
	PartitionKeyAttribute string
	SortKeyAttribute      string
}

func (s Index) getPartitionKeyAttribute() string {
	if len(s.PartitionKeyAttribute) > 0 {
		return s.PartitionKeyAttribute
	}
	return pk
}

func (s Index) getSortKeyAttribute() string {
	if len(s.SortKeyAttribute) > 0 {
		return s.SortKeyAttribute
	}
	return ""
}

func (s Index) hasSortKey() bool {
	return len(s.SortKeyAttribute) > 0
}
