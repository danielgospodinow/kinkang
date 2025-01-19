package balancer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoundRobinBalancer_Balance(t *testing.T) {
	t.Parallel()

	t.Run("empty assignments", func(t *testing.T) {
		t.Parallel()

		balancer := NewRoundRobinBalancer()
		assignments, err := balancer.Balance("test-topic", 6, map[int32][]int32{}, nil)
		require.NoError(t, err)
		assert.Empty(t, assignments)
	})

	t.Run("single partition, single replica", func(t *testing.T) {
		t.Parallel()

		balancer := NewRoundRobinBalancer()
		assignments, err := balancer.Balance("test-topic", 6, nil, map[int32][]int32{
			0: {0},
		})
		require.NoError(t, err)
		assert.Equal(t, map[int32][]int32{
			0: {0},
		}, assignments)
	})

	t.Run("single partition, multiple replicas", func(t *testing.T) {
		t.Parallel()

		balancer := NewRoundRobinBalancer()
		assignments, err := balancer.Balance("test-topic", 6, nil, map[int32][]int32{
			0: {0, 1, 2},
		})
		require.NoError(t, err)
		assert.Equal(t, map[int32][]int32{
			0: {0, 1, 2},
		}, assignments)
	})

	t.Run("multiple partitions, single replica", func(t *testing.T) {
		t.Parallel()

		balancer := NewRoundRobinBalancer()
		assignments, err := balancer.Balance("test-topic", 6, nil, map[int32][]int32{
			0: {0},
			1: {0},
			2: {0},
		})
		require.NoError(t, err)
		assert.Equal(t, map[int32][]int32{
			0: {0},
			1: {1},
			2: {2},
		}, assignments)
	})

	t.Run("multiple partitions, multiple replicas", func(t *testing.T) {
		t.Parallel()

		balancer := NewRoundRobinBalancer()
		assignments, err := balancer.Balance("test-topic", 6, nil, map[int32][]int32{
			0: {0, 1, 2},
			1: {0, 1, 2},
			2: {0, 1, 2},
			3: {0, 1, 2},
			4: {0, 1, 2},
			5: {0, 1, 2},
			6: {0, 1, 2},
			7: {0, 1, 2},
			8: {0, 1, 2},
			9: {0, 1, 2},
		})
		require.NoError(t, err)
		assert.Equal(t, map[int32][]int32{
			0: {0, 1, 2},
			1: {1, 2, 3},
			2: {2, 3, 4},
			3: {3, 4, 5},
			4: {4, 5, 0},
			5: {5, 0, 1},
			6: {0, 1, 2},
			7: {1, 2, 3},
			8: {2, 3, 4},
			9: {3, 4, 5},
		}, assignments)
	})
}
