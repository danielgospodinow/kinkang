package balancer

// ConvertAssignmetsMapToMatrix converts a map of partition assignments to a matrix.
func ConvertAssignmetsMapToMatrix(assignments map[int32][]int32) [][]int32 {
	var matrix [][]int32
	for _, replicas := range assignments {
		matrix = append(matrix, replicas)
	}
	return matrix
}
