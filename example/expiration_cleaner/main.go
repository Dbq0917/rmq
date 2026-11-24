package main

import (
	"log"
	"time"

	"github.com/adjust/rmq/v5"
)

func main() {
	connection, err := rmq.OpenConnection("expiration-cleaner", "tcp", "localhost:6379", 2, nil)
	if err != nil {
		panic(err)
	}

	// Producers publish deliveries with a TTL. Expired deliveries stay in the
	// ready list until an on-demand cleaning pass removes them.
	queue, err := connection.OpenQueue("things")
	if err != nil {
		panic(err)
	}
	if err := queue.PublishWithTTL(time.Hour, "delivery with a one hour TTL"); err != nil {
		log.Printf("failed to publish: %s", err)
	}

	cleaner := rmq.NewCleaner(connection)

	// Run the cheap on-demand cleanup on the same schedule as the regular
	// cleaner. Small queues are skipped entirely, and large queues are only
	// scanned from the head within a bounded window, so this never blocks
	// consumers with a full scan.
	for range time.Tick(time.Minute) {
		removed, err := cleaner.CleanExpired(
			rmq.DefaultExpiryCleanThreshold,
			rmq.DefaultExpiryCleanMax,
		)
		if err != nil {
			log.Printf("failed to clean expired deliveries: %s", err)
			continue
		}
		log.Printf("removed %d expired deliveries", removed)
	}
}
