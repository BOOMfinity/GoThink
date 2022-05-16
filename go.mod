module github.com/BOOMfinity-Developers/GoThink

go 1.18

require (
	github.com/cheggaaa/pb v1.0.29
	github.com/hashicorp/go-version v1.4.0
	github.com/klauspost/compress v1.15.2
	github.com/klauspost/pgzip v1.2.5
	github.com/segmentio/encoding v0.3.5
	github.com/urfave/cli/v2 v2.5.1
	gopkg.in/rethinkdb/rethinkdb-go.v6 v6.2.1
)

require (
	github.com/cenkalti/backoff/v4 v4.1.3 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.1 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/mattn/go-runewidth v0.0.4 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/segmentio/asm v1.1.3 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	golang.org/x/crypto v0.0.0-20220507011949-2cf3adece122 // indirect
	golang.org/x/net v0.0.0-20211112202133-69e39bad7dc2 // indirect
	golang.org/x/sys v0.0.0-20211110154304-99a53858aa08 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
)

replace gopkg.in/rethinkdb/rethinkdb-go.v6 v6.2.1 => github.com/BOOMfinity-Developers/rethinkdb-go/v6 v6.2.2-0.20220509153636-c24b6ea38ec7
