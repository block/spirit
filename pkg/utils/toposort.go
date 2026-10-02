package utils

import "slices"

// TopologicalOrder returns nodes reordered so that every node comes after the
// nodes it depends on. dependsOn maps a node to the nodes that must precede
// it; a dependency that is not itself in nodes, or is the node itself, is
// ignored.
//
// The order is deterministic: among the nodes whose dependencies are all
// placed, the earliest in nodes goes next, so nodes with no dependency between
// them keep their input order. A dependency cycle cannot be satisfied; when
// every remaining node still waits on another, the earliest remaining node that
// another remaining node waits on is placed anyway, so the broken edge is one
// of the cycle's and no node is placed ahead of a dependency it could have
// followed. nodes must not contain duplicates.
func TopologicalOrder[T comparable](nodes []T, dependsOn map[T][]T) []T {
	inSet := make(map[T]bool, len(nodes))
	for _, n := range nodes {
		inSet[n] = true
	}
	placed := make(map[T]bool, len(nodes))
	// waitsOn reports whether n has a dependency in nodes not yet placed,
	// calling visit with each.
	waitsOn := func(n T, visit func(T)) bool {
		waiting := false
		for _, d := range dependsOn[n] {
			if d != n && inSet[d] && !placed[d] {
				waiting = true
				if visit != nil {
					visit(d)
				}
			}
		}
		return waiting
	}

	order := make([]T, 0, len(nodes))
	remaining := slices.Clone(nodes)
	for len(remaining) > 0 {
		next := slices.IndexFunc(remaining, func(n T) bool { return !waitsOn(n, nil) })
		if next < 0 {
			// A cycle: every remaining node waits on another remaining node,
			// so at least one remaining node is waited on.
			waitedOn := make(map[T]bool)
			for _, n := range remaining {
				waitsOn(n, func(d T) { waitedOn[d] = true })
			}
			next = slices.IndexFunc(remaining, func(n T) bool { return waitedOn[n] })
		}
		n := remaining[next]
		placed[n] = true
		order = append(order, n)
		remaining = slices.Delete(remaining, next, next+1)
	}
	return order
}
