package mgo_test

func stop(pid int) (err error) {
	panicOnWindows() // Always does.
	return nil
}

func cont(pid int) (err error) {
	panicOnWindows() // Always does.
	return nil
}
