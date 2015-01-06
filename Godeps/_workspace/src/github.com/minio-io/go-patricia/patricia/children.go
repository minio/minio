// Copyright (c) 2014 The go-patricia AUTHORS
//
// Use of this source code is governed by The MIT License
// that can be found in the LICENSE file.

package patricia

// Max prefix length that is kept in a single trie node.
var MaxPrefixPerNode = 10

// Max children to keep in a node in the sparse mode.
const MaxChildrenPerSparseNode = 8

type ChildList interface {
	length() int
	head() *Trie
	add(child *Trie) ChildList
	replace(b byte, child *Trie)
	remove(child *Trie)
	next(b byte) *Trie
	walk(prefix *Prefix, visitor VisitorFunc) error
}

type SparseChildList struct {
	Children []*Trie
}

func newSparseChildList() ChildList {
	return &SparseChildList{
		Children: make([]*Trie, 0, MaxChildrenPerSparseNode),
	}
}

func (list *SparseChildList) length() int {
	return len(list.Children)
}

func (list *SparseChildList) head() *Trie {
	return list.Children[0]
}

func (list *SparseChildList) add(child *Trie) ChildList {
	// Search for an empty spot and insert the child if possible.
	if len(list.Children) != cap(list.Children) {
		list.Children = append(list.Children, child)
		return list
	}

	// Otherwise we have to transform to the dense list type.
	return newDenseChildList(list, child)
}

func (list *SparseChildList) replace(b byte, child *Trie) {
	// Seek the child and replace it.
	for i, node := range list.Children {
		if node.InternalPrefix[0] == b {
			list.Children[i] = child
			return
		}
	}
}

func (list *SparseChildList) remove(child *Trie) {
	for i, node := range list.Children {
		if node.InternalPrefix[0] == child.InternalPrefix[0] {
			list.Children = append(list.Children[:i], list.Children[i+1:]...)
			return
		}
	}

	// This is not supposed to be reached.
	panic("removing non-existent child")
}

func (list *SparseChildList) next(b byte) *Trie {
	for _, child := range list.Children {
		if child.InternalPrefix[0] == b {
			return child
		}
	}
	return nil
}

func (list *SparseChildList) walk(prefix *Prefix, visitor VisitorFunc) error {
	for _, child := range list.Children {
		*prefix = append(*prefix, child.InternalPrefix...)
		if child.InternalItem != nil {
			err := visitor(*prefix, child.InternalItem)
			if err != nil {
				if err == SkipSubtree {
					*prefix = (*prefix)[:len(*prefix)-len(child.InternalPrefix)]
					continue
				}
				*prefix = (*prefix)[:len(*prefix)-len(child.InternalPrefix)]
				return err
			}
		}

		err := child.InternalChildren.walk(prefix, visitor)
		*prefix = (*prefix)[:len(*prefix)-len(child.InternalPrefix)]
		if err != nil {
			return err
		}
	}

	return nil
}

type DenseChildList struct {
	Min      int
	Max      int
	Children []*Trie
}

func newDenseChildList(list *SparseChildList, child *Trie) ChildList {
	var (
		min int = 255
		max int = 0
	)
	for _, child := range list.Children {
		b := int(child.InternalPrefix[0])
		if b < min {
			min = b
		}
		if b > max {
			max = b
		}
	}

	b := int(child.InternalPrefix[0])
	if b < min {
		min = b
	}
	if b > max {
		max = b
	}

	children := make([]*Trie, max-min+1)
	for _, child := range list.Children {
		children[int(child.InternalPrefix[0])-min] = child
	}
	children[int(child.InternalPrefix[0])-min] = child

	return &DenseChildList{min, max, children}
}

func (list *DenseChildList) length() int {
	return list.Max - list.Min + 1
}

func (list *DenseChildList) head() *Trie {
	return list.Children[0]
}

func (list *DenseChildList) add(child *Trie) ChildList {
	b := int(child.InternalPrefix[0])

	switch {
	case list.Min <= b && b <= list.Max:
		if list.Children[b-list.Min] != nil {
			panic("dense child list collision detected")
		}
		list.Children[b-list.Min] = child

	case b < list.Min:
		children := make([]*Trie, list.Max-b+1)
		children[0] = child
		copy(children[list.Min-b:], list.Children)
		list.Children = children
		list.Min = b

	default: // b > list.max
		children := make([]*Trie, b-list.Min+1)
		children[b-list.Min] = child
		copy(children, list.Children)
		list.Children = children
		list.Max = b
	}

	return list
}

func (list *DenseChildList) replace(b byte, child *Trie) {
	list.Children[int(b)-list.Min] = nil
	list.Children[int(child.InternalPrefix[0])-list.Min] = child
}

func (list *DenseChildList) remove(child *Trie) {
	i := int(child.InternalPrefix[0]) - list.Min
	if list.Children[i] == nil {
		// This is not supposed to be reached.
		panic("removing non-existent child")
	}
	list.Children[i] = nil
}

func (list *DenseChildList) next(b byte) *Trie {
	i := int(b)
	if i < list.Min || list.Max < i {
		return nil
	}
	return list.Children[i-list.Min]
}

func (list *DenseChildList) walk(prefix *Prefix, visitor VisitorFunc) error {
	for _, child := range list.Children {
		if child == nil {
			continue
		}
		*prefix = append(*prefix, child.InternalPrefix...)
		if child.InternalItem != nil {
			if err := visitor(*prefix, child.InternalItem); err != nil {
				if err == SkipSubtree {
					*prefix = (*prefix)[:len(*prefix)-len(child.InternalPrefix)]
					continue
				}
				*prefix = (*prefix)[:len(*prefix)-len(child.InternalPrefix)]
				return err
			}
		}

		err := child.InternalChildren.walk(prefix, visitor)
		*prefix = (*prefix)[:len(*prefix)-len(child.InternalPrefix)]
		if err != nil {
			return err
		}
	}

	return nil
}
