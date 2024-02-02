package lookuptable

type LookupTable struct {
	StartupHook func()
}

var GlobalLookupTable LookupTable
