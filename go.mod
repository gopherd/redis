module github.com/gopherd/redis

go 1.16

require (
	github.com/go-redis/redis/v8 v8.10.0
	github.com/gopherd/doge v0.0.8
)

replace github.com/gopherd/doge => ../doge
