package store

import (
	"sync"
)

type Store[T any] struct {
	cache map[string]T
	mu    sync.Mutex
}

func New[T any]() Store[T] {
	return Store[T]{
		cache: make(map[string]T),
		mu:    sync.Mutex{},
	}
}

func (s *Store[T]) Store(key string, value T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cache[key] = value
}

func (s *Store[T]) Load(key string) (T, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	value, ok := s.cache[key]
	return value, ok
}

func (s *Store[T]) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.cache, key)
}
