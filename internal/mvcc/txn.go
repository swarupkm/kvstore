package mvcc

import "time"

// TxType distinguishes read-only snapshots from read-write transactions
type TxType int

const (
	ReadOnly  TxType = iota
	ReadWrite
)

// Tx is a transaction handle — snapshot ID is fixed at creation time
type Tx struct {
	ID       uint64
	Type     TxType
	store    *MVCCStore
	writes   map[string]Version // buffered writes, not yet committed
	done     bool
}

func (tx *Tx) Get(key string) (string, bool) {
	// check buffered writes first
	if v, ok := tx.writes[key]; ok {
		if v.Deleted {
			return "", false
		}
		return v.Value, true
	}
	return tx.store.getAt(key, tx.ID)
}

func (tx *Tx) Set(key, value string) {
	if tx.Type == ReadOnly {
		panic("cannot write in a read-only transaction")
	}
	tx.writes[key] = Version{
		TxID:  tx.ID,
		Value: value,
	}
}

func (tx *Tx) SetWithTTL(key, value string, ttl time.Duration) {
	if tx.Type == ReadOnly {
		panic("cannot write in a read-only transaction")
	}
	tx.writes[key] = Version{
		TxID:      tx.ID,
		Value:     value,
		ExpiresAt: time.Now().Add(ttl),
	}
}

func (tx *Tx) Delete(key string) {
	if tx.Type == ReadOnly {
		panic("cannot write in a read-only transaction")
	}
	tx.writes[key] = Version{
		TxID:    tx.ID,
		Deleted: true,
	}
}

func (tx *Tx) Commit() error {
	if tx.done {
		return nil
	}
	tx.done = true
	return tx.store.commit(tx)
}

func (tx *Tx) Rollback() {
	tx.done = true
	tx.writes = nil
	tx.store.releaseTx(tx.ID)
}

// Writes returns a snapshot of buffered writes for the caller to inspect
func (tx *Tx) Writes() map[string]Version {
	snapshot := make(map[string]Version, len(tx.writes))
	for k, v := range tx.writes {
		snapshot[k] = v
	}
	return snapshot
}