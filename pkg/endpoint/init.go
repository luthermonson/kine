package endpoint

import (
	// Import all the default drivers
	_ "github.com/k3s-io/kine/pkg/drivers/beads"
	_ "github.com/k3s-io/kine/pkg/drivers/memory"
	_ "github.com/k3s-io/kine/pkg/drivers/mysql"
	_ "github.com/k3s-io/kine/pkg/drivers/nats"
	_ "github.com/k3s-io/kine/pkg/drivers/pgsql"
	_ "github.com/k3s-io/kine/pkg/drivers/remote"
	_ "github.com/k3s-io/kine/pkg/drivers/split"
	_ "github.com/k3s-io/kine/pkg/drivers/sqlite"
	// NOTE(k3s-on-beads): the t4 driver is intentionally omitted on this
	// branch. It pulls in github.com/DataDog/zstd, whose bundled zstd C
	// library collides at link time with dolthub/gozstd (527 duplicate
	// symbols) once the embedded-Dolt beads driver is compiled in. We don't
	// need t4 for the beads frankenstein; re-add it only once the zstd
	// double-link is resolved upstream.
	// _ "github.com/k3s-io/kine/pkg/drivers/t4"
)
