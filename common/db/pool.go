package db

func CreatePool(poolType string, poolSize int, ip string, keyspace string) interface{} {
	switch poolType {
	case "custom":
		return createCustomPool(poolSize, ip, keyspace)
	case "custom-with-channels":
		return createCustomPoolWithChannels(poolSize, ip, keyspace)
	case "standard":
		return createStandardPool(poolSize, ip, keyspace)
	}

	panic("Unsupported pool type to method CreatePool()")
}

func createCustomPool(poolSize int, ip string, keyspace string) *CustomPool {
	// This pool implementation isn't good, it keeps on creating new sessions if gets short of them
	pool := &CustomPool{}
	pool.Init(ip, keyspace)
	err := pool.Connect(poolSize)
	if err != nil {
		// change this to error
		panic("Failed to create custom pool")
	}
	return pool
}

func createCustomPoolWithChannels(poolSize int, ip string, keyspace string) *CustomPoolWithChannels {
	// This pool implementation isn't good, it keeps on creating new sessions if gets short of them
	pool := &CustomPoolWithChannels{}
	pool.Init(ip, keyspace)
	err := pool.Connect(poolSize)
	if err != nil {
		// change this to error
		panic("Failed to create custom pool")
	}
	return pool
}

func createStandardPool(poolSize int, ip string, keyspace string) *StandardPool {
	// This pool implementation isn't good, it keeps on creating new sessions if gets short of them
	pool := &StandardPool{}
	pool.Init(ip, keyspace)
	err := pool.Connect(poolSize)
	if err != nil {
		// change this to error
		panic("Failed to create standard pool")
	}
	return pool
}
