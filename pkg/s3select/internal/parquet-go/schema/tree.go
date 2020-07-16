/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package schema

import (
	"fmt"
	"strings"

	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
)

func updateMaxDLRL(schemaMap map[string]*Element, maxDL, maxRL int64) {
	for _, element := range schemaMap {
		element.MaxDefinitionLevel = maxDL
		element.MaxRepetitionLevel = maxRL
		if *element.RepetitionType != parquet.FieldRepetitionType_REQUIRED {
			element.MaxDefinitionLevel++
			if *element.RepetitionType == parquet.FieldRepetitionType_REPEATED {
				element.MaxRepetitionLevel++
			}
		}

		if element.Children != nil {
			updateMaxDLRL(element.Children.schemaMap, element.MaxDefinitionLevel, element.MaxRepetitionLevel)
		}
	}
}

func toParquetSchema(tree *Tree, treePrefix string, schemaPrefix string, schemaList *[]*parquet.SchemaElement, valueElements *[]*Element) (err error) {
	tree.Range(func(name string, element *Element) bool {
		pathInTree := name
		if treePrefix != "" {
			pathInTree = treePrefix + "." + name
		}

		if element.Type == nil && element.ConvertedType == nil && element.Children == nil {
			err = fmt.Errorf("%v: group element must have children", pathInTree)
			return false
		}

		if element.ConvertedType != nil {
			switch *element.ConvertedType {
			case parquet.ConvertedType_LIST:
				// Supported structure.
				// <REQUIRED|OPTIONAL> group <name> (LIST) {
				//   REPEATED group list {
				//     <REQUIRED|OPTIONAL> <element-type> element;
				//   }
				// }

				if element.Type != nil {
					err = fmt.Errorf("%v: type must be nil for LIST ConvertedType", pathInTree)
					return false
				}

				if element.Children == nil || element.Children.Length() != 1 {
					err = fmt.Errorf("%v: children must have one element only for LIST ConvertedType", pathInTree)
					return false
				}

				listElement, ok := element.Children.Get("list")
				if !ok {
					err = fmt.Errorf("%v: missing group element 'list' for LIST ConvertedType", pathInTree)
					return false
				}

				if listElement.Name != "list" {
					err = fmt.Errorf("%v.list: name must be 'list'", pathInTree)
					return false
				}

				if *listElement.RepetitionType != parquet.FieldRepetitionType_REPEATED {
					err = fmt.Errorf("%v.list: repetition type must be REPEATED type", pathInTree)
					return false
				}

				if listElement.Type != nil || listElement.ConvertedType != nil {
					err = fmt.Errorf("%v.list: type and converted type must be nil", pathInTree)
					return false
				}

				if listElement.Children == nil || listElement.Children.Length() != 1 {
					err = fmt.Errorf("%v.list.element: not found", pathInTree)
					return false
				}

				valueElement, ok := listElement.Children.Get("element")
				if !ok {
					err = fmt.Errorf("%v.list.element: not found", pathInTree)
					return false
				}

				if valueElement.Name != "element" {
					err = fmt.Errorf("%v.list.element: name must be 'element'", pathInTree)
					return false
				}

			case parquet.ConvertedType_MAP:
				// Supported structure:
				// <REQUIRED|OPTIONAL> group <name> (MAP) {
				//   REPEATED group key_value {
				//     REQUIRED <key-type> key;
				//     <REQUIRED|OPTIONAL> <value-type> value;
				//   }
				// }

				if element.Type != nil {
					err = fmt.Errorf("%v: type must be nil for MAP ConvertedType", pathInTree)
					return false
				}

				if element.Children == nil || element.Children.Length() != 1 {
					err = fmt.Errorf("%v: children must have one element only for MAP ConvertedType", pathInTree)
					return false
				}

				keyValueElement, ok := element.Children.Get("key_value")
				if !ok {
					err = fmt.Errorf("%v: missing group element 'key_value' for MAP ConvertedType", pathInTree)
					return false
				}

				if keyValueElement.Name != "key_value" {
					err = fmt.Errorf("%v.key_value: name must be 'key_value'", pathInTree)
					return false
				}

				if *keyValueElement.RepetitionType != parquet.FieldRepetitionType_REPEATED {
					err = fmt.Errorf("%v.key_value: repetition type must be REPEATED type", pathInTree)
					return false
				}

				if keyValueElement.Children == nil || keyValueElement.Children.Length() < 1 || keyValueElement.Children.Length() > 2 {
					err = fmt.Errorf("%v.key_value: children must have 'key' and optionally 'value' elements for MAP ConvertedType", pathInTree)
					return false
				}

				keyElement, ok := keyValueElement.Children.Get("key")
				if !ok {
					err = fmt.Errorf("%v.key_value: missing 'key' element for MAP ConvertedType", pathInTree)
					return false
				}

				if keyElement.Name != "key" {
					err = fmt.Errorf("%v.key_value.key: name must be 'key'", pathInTree)
					return false
				}

				if *keyElement.RepetitionType != parquet.FieldRepetitionType_REQUIRED {
					err = fmt.Errorf("%v.key_value: repetition type must be REQUIRED type", pathInTree)
					return false
				}

				if keyValueElement.Children.Length() == 2 {
					valueElement, ok := keyValueElement.Children.Get("value")
					if !ok {
						err = fmt.Errorf("%v.key_value: second element must be 'value' element for MAP ConvertedType", pathInTree)
						return false
					}

					if valueElement.Name != "value" {
						err = fmt.Errorf("%v.key_value.value: name must be 'value'", pathInTree)
						return false
					}
				}

			case parquet.ConvertedType_UTF8, parquet.ConvertedType_UINT_8, parquet.ConvertedType_UINT_16:
				fallthrough
			case parquet.ConvertedType_UINT_32, parquet.ConvertedType_UINT_64, parquet.ConvertedType_INT_8:
				fallthrough
			case parquet.ConvertedType_INT_16, parquet.ConvertedType_INT_32, parquet.ConvertedType_INT_64:
				if element.Type == nil {
					err = fmt.Errorf("%v: ConvertedType %v must have Type value", pathInTree, element.ConvertedType)
					return false
				}

			default:
				err = fmt.Errorf("%v: unsupported ConvertedType %v", pathInTree, element.ConvertedType)
				return false
			}
		}

		element.PathInTree = pathInTree
		element.PathInSchema = element.Name
		if schemaPrefix != "" {
			element.PathInSchema = schemaPrefix + "." + element.Name
		}

		if element.Type != nil {
			*valueElements = append(*valueElements, element)
		}

		*schemaList = append(*schemaList, &element.SchemaElement)
		if element.Children != nil {
			element.numChildren = int32(element.Children.Length())
			err = toParquetSchema(element.Children, element.PathInTree, element.PathInSchema, schemaList, valueElements)
		}

		return (err == nil)
	})

	return err
}

// Tree - represents tree of schema.  Tree preserves order in which elements are added.
type Tree struct {
	schemaMap map[string]*Element
	keys      []string
	readOnly  bool
}

// String - stringify this tree.
func (tree *Tree) String() string {
	var s []string
	tree.Range(func(name string, element *Element) bool {
		s = append(s, fmt.Sprintf("%v: %v", name, element))
		return true
	})

	return "{" + strings.Join(s, ", ") + "}"
}

// Length - returns length of tree.
func (tree *Tree) Length() int {
	return len(tree.keys)
}

func (tree *Tree) travel(pathSegments []string) (pathSegmentIndex int, pathSegment string, currElement *Element, parentTree *Tree, found bool) {
	parentTree = tree
	for pathSegmentIndex, pathSegment = range pathSegments {
		if tree == nil {
			found = false
			break
		}

		var tmpCurrElement *Element
		if tmpCurrElement, found = tree.schemaMap[pathSegment]; !found {
			break
		}
		currElement = tmpCurrElement

		parentTree = tree
		tree = currElement.Children
	}

	return
}

// ReadOnly - returns whether this tree is read only or not.
func (tree *Tree) ReadOnly() bool {
	return tree.readOnly
}

// Get - returns the element stored for name.
func (tree *Tree) Get(name string) (element *Element, ok bool) {
	pathSegments := strings.Split(name, ".")
	for _, pathSegment := range pathSegments {
		if tree == nil {
			element = nil
			ok = false
			break
		}

		if element, ok = tree.schemaMap[pathSegment]; !ok {
			break
		}

		tree = element.Children
	}

	return element, ok
}

// Set - adds or sets element to name.
func (tree *Tree) Set(name string, element *Element) error {
	if tree.readOnly {
		return fmt.Errorf("read only tree")
	}

	pathSegments := strings.Split(name, ".")
	if err := validataPathSegments(pathSegments); err != nil {
		return err
	}

	i, pathSegment, currElement, parentTree, found := tree.travel(pathSegments)

	if !found {
		if i != len(pathSegments)-1 {
			return fmt.Errorf("parent %v does not exist", strings.Join(pathSegments[:i+1], "."))
		}

		if currElement == nil {
			parentTree = tree
		} else {
			if currElement.Type != nil {
				return fmt.Errorf("parent %v is not group element", strings.Join(pathSegments[:i], "."))
			}

			if currElement.Children == nil {
				currElement.Children = NewTree()
			}
			parentTree = currElement.Children
		}

		parentTree.keys = append(parentTree.keys, pathSegment)
	}

	parentTree.schemaMap[pathSegment] = element
	return nil
}

// Delete - deletes name and its element.
func (tree *Tree) Delete(name string) {
	if tree.readOnly {
		panic(fmt.Errorf("read only tree"))
	}

	pathSegments := strings.Split(name, ".")

	_, pathSegment, _, parentTree, found := tree.travel(pathSegments)

	if found {
		for i := range parentTree.keys {
			if parentTree.keys[i] == pathSegment {
				copy(parentTree.keys[i:], parentTree.keys[i+1:])
				parentTree.keys = parentTree.keys[:len(parentTree.keys)-1]
				break
			}
		}

		delete(parentTree.schemaMap, pathSegment)
	}
}

// Range - calls f sequentially for each name and its element. If f returns false, range stops the iteration.
func (tree *Tree) Range(f func(name string, element *Element) bool) {
	for _, name := range tree.keys {
		if !f(name, tree.schemaMap[name]) {
			break
		}
	}
}

// ToParquetSchema - returns list of parquet SchemaElement and list of elements those stores values.
func (tree *Tree) ToParquetSchema() (schemaList []*parquet.SchemaElement, valueElements []*Element, err error) {
	if tree.readOnly {
		return nil, nil, fmt.Errorf("read only tree")
	}

	updateMaxDLRL(tree.schemaMap, 0, 0)

	var schemaElements []*parquet.SchemaElement
	if err = toParquetSchema(tree, "", "", &schemaElements, &valueElements); err != nil {
		return nil, nil, err
	}

	tree.readOnly = true

	numChildren := int32(len(tree.keys))
	schemaList = append(schemaList, &parquet.SchemaElement{
		Name:           "schema",
		RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
		NumChildren:    &numChildren,
	})
	schemaList = append(schemaList, schemaElements...)
	return schemaList, valueElements, nil
}

// NewTree - creates new schema tree.
func NewTree() *Tree {
	return &Tree{
		schemaMap: make(map[string]*Element),
	}
}
