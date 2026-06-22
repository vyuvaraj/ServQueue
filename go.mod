module servqueue

go 1.26.4

require github.com/tetratelabs/wazero v1.12.0

require github.com/vyuvaraj/ServShared v0.0.0

require (
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	golang.org/x/sys v0.44.0 // indirect
)

replace github.com/vyuvaraj/ServShared => ../ServShared
