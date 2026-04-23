package store

import "github.com/google/btree"

type btreeItem string

func (a btreeItem) Less(b btree.Item) bool {
	return a < b.(btreeItem)
}

type index struct {
	tree *btree.BTree
}

func newIndex() *index {
	return &index{tree: btree.New(32)}
}

func (i *index) add(key string){
	i.tree.ReplaceOrInsert(btreeItem(key))
}

func (i *index) remove(key string){
	i.tree.Delete(btreeItem(key))
}

func (i *index) keys() []string {
	result := make([]string, 0, i.tree.Len())
	i.tree.Ascend(func(item btree.Item) bool {
		result = append(result, string(item.(btreeItem)))
		return true
	})
	return result
}

func (i * index) rangeKeys(start, end string) []string {
	var result []string
	i.tree.AscendRange(btreeItem(start), btreeItem(end), func(item btree.Item) bool {
		result = append(result, string(item.(btreeItem)))
		return true
	})
	if i.tree.Has(btreeItem(end)) {
		result = append(result, end)
	}
	return result
}