package main

type store struct {
	entries map[string]any
}

func newStore() *store {
	return &store{
		entries: map[string]any{},
	}
}

func (st *store) getItem(key string) any {
	item, ok := st.entries[key]

	if ok {
		return item
	}

	return nil
}

func (st *store) setItem(key string, value any) {
	st.entries[key] = value
}
