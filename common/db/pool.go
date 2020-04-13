package db

func CreatePool(poolType string, ip string, keyspace string) interface{} {
	switch poolType {
	case "custom":
		return createCustomPool(ip, keyspace)
	case "standard":
		return createStandardPool(ip, keyspace)
	}

	panic("Unsupported pool type to method CreatePool()")
}

func createCustomPool(ip string, keyspace string) *CustomPool {
	// This pool implementation isn't good, it keeps on creating new sessions if gets short of them
	pool := &CustomPool{}
	pool.Init(ip, keyspace)
	return pool
}

func createStandardPool(ip string, keyspace string) *StandardPool {
	// This pool implementation isn't good, it keeps on creating new sessions if gets short of them
	pool := &StandardPool{}
	pool.Init(ip, keyspace)
	return pool
}
