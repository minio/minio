package hosttree

type byWeight []hostEntry

func (he byWeight) Len() int           { return len(he) }
func (he byWeight) Less(i, j int) bool { return he[i].weight.Cmp(he[j].weight) < 0 }
func (he byWeight) Swap(i, j int)      { he[i], he[j] = he[j], he[i] }
