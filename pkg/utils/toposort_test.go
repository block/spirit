package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopologicalOrder(t *testing.T) {
	tests := []struct {
		name      string
		nodes     []string
		dependsOn map[string][]string
		want      []string
	}{
		{
			name:  "empty",
			nodes: nil,
			want:  []string{},
		},
		{
			name:  "no dependencies keep input order",
			nodes: []string{"c", "a", "b"},
			want:  []string{"c", "a", "b"},
		},
		{
			name:      "dependency moves ahead of its dependent",
			nodes:     []string{"child", "parent"},
			dependsOn: map[string][]string{"child": {"parent"}},
			want:      []string{"parent", "child"},
		},
		{
			name:      "chain",
			nodes:     []string{"a", "b", "c"},
			dependsOn: map[string][]string{"a": {"b"}, "b": {"c"}},
			want:      []string{"c", "b", "a"},
		},
		{
			name:      "independent nodes stay in input order around a chain",
			nodes:     []string{"a", "b", "c", "d"},
			dependsOn: map[string][]string{"b": {"d"}},
			want:      []string{"a", "c", "d", "b"},
		},
		{
			name:      "dependency outside the set is ignored",
			nodes:     []string{"a", "b"},
			dependsOn: map[string][]string{"a": {"other"}},
			want:      []string{"a", "b"},
		},
		{
			name:      "self dependency is ignored",
			nodes:     []string{"a", "b"},
			dependsOn: map[string][]string{"a": {"a"}},
			want:      []string{"a", "b"},
		},
		{
			name:      "two-node cycle keeps input order",
			nodes:     []string{"a", "b"},
			dependsOn: map[string][]string{"a": {"b"}, "b": {"a"}},
			want:      []string{"a", "b"},
		},
		{
			// c depends on a, which is in a cycle with b. Breaking the cycle
			// at a (the earliest node waited on) keeps c after a; breaking at
			// the earliest remaining node would have placed c first. Once a
			// is placed, c is free and precedes b in input order.
			name:      "cycle is broken at a node that is waited on",
			nodes:     []string{"c", "a", "b"},
			dependsOn: map[string][]string{"c": {"a"}, "a": {"b"}, "b": {"a"}},
			want:      []string{"a", "c", "b"},
		},
		{
			name:      "node after a cycle still follows its dependency",
			nodes:     []string{"a", "b", "c"},
			dependsOn: map[string][]string{"a": {"b"}, "b": {"a"}, "c": {"b"}},
			want:      []string{"a", "b", "c"},
		},
		{
			name:      "several dependencies all precede the dependent",
			nodes:     []string{"x", "p", "q"},
			dependsOn: map[string][]string{"x": {"p", "q"}},
			want:      []string{"p", "q", "x"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := append([]string(nil), tt.nodes...)
			got := TopologicalOrder(nodes, tt.dependsOn)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.nodes, nodes, "input must not be reordered")
		})
	}
}
