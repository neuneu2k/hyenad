/*
Copyright 2016 Assoba S.A.S.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package hyenad

import (
	"gopkg.in/tchap/go-patricia.v2/patricia"
	"strconv"
	"strings"
	"sync"
)

type RoutingTree struct {
	trie *patricia.Trie
	lock sync.Mutex
}

type RoutingTreeUpdate struct {
	Deletes  []string           `json:"delete,omitempty"`
	Services map[string]Simple  `json:"services,omitempty"`
	Shards   map[string]Sharded `json:"shards,omitempty"`
}

func NewRoutingTree() *RoutingTree {
	return &RoutingTree{
		trie: patricia.NewTrie(),
	}
}

func (r *RoutingTree) Apply(update RoutingTreeUpdate) {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, delete := range update.Deletes {
		r.trie.Delete(patricia.Prefix(delete))
	}
	for prefix, service := range update.Services {
		r.trie.Insert(patricia.Prefix(prefix), service)
	}
	for prefix, shard := range update.Services {
		r.trie.Insert(patricia.Prefix(prefix), shard)
	}
}

func (r *RoutingTree) UpsertSimpleRule(prefix string, rule Simple) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.trie.Insert(patricia.Prefix(prefix), rule)
}

func (r *RoutingTree) RemoveRule(prefix string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.trie.Delete(patricia.Prefix(prefix))
}

func (r *RoutingTree) UpsertShardedRule(prefix string, rule Sharded) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.trie.Insert(patricia.Prefix(prefix), rule)
}

func (r *RoutingTree) Route(destination string) Addresses {
	if strings.HasPrefix(destination, "x:") {
		parts := strings.Split(destination, "/")
		if len(parts) < 2 {
			return Addresses([]Address{})
		} else {
			nid, err := strconv.ParseInt(parts[0], 10, 32)
			pid, err := strconv.ParseInt(parts[1], 10, 32)
			if err != nil {
				return Addresses([]Address{})
			}
			return Addresses([]Address{Address{uint32(nid), uint32(pid)}})
		}
	} else {
		r.lock.Lock()
		defer r.lock.Unlock()
		var longestKey patricia.Prefix
		var ruleItem patricia.Item
		r.trie.VisitPrefixes(patricia.Prefix(destination), func(key patricia.Prefix, value patricia.Item) error {
			if len(key) > len(longestKey) {
				longestKey = key
				ruleItem = value
			}
			return nil
		})
		switch t := ruleItem.(type) {
		case Sharded:
			{
				shardParts := strings.Split(destination[len(longestKey):], "/")
				if len(shardParts) < 1 {
					return Addresses([]Address{})
				}
				shard := shardParts[0]
				for _, r := range t {
					if shard >= r.From && shard <= r.To {
						return r.Addresses()
					}
				}
			}
		case Simple:
			{
				return t.Addresses()
			}
		}
	}
	return Addresses([]Address{})
}

type Sharded []ShardEntry

type Simple struct {
	Targets Addresses `json:"targets,omitempty"`
}

func (s *Simple) Addresses() Addresses {
	return s.Targets
}

type ShardEntry struct {
	From    string    `json:"from,omitempty"`
	To      string    `json:"to,omitempty"`
	Targets Addresses `json:"targets,omitempty"`
}

func (s *ShardEntry) Addresses() Addresses {
	return s.Targets
}
