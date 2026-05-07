package query

// StatementType identifies which kind of statement was parsed
type StatementType int

const (
	StmtSet StatementType = iota
	StmtGet
	StmtDelete
	StmtSelect
)

// Statement is the root AST node — every parsed query becomes one
type Statement struct {
	Type      StatementType
	Key       string     // for GET, DELETE, SET
	Value     string     // for SET
	Condition *Condition // for SELECT
}

// Condition represents the WHERE clause of a SELECT
type Condition struct {
	Op    ConditionOp
	Key   string // left-hand side
	Value string // right-hand side
	End   string // for BETWEEN
}

type ConditionOp int

const (
	OpEqual      ConditionOp = iota // key = "x"
	OpStartsWith                    // key STARTS_WITH "x"
	OpBetween                       // key BETWEEN "x" AND "y"
)