package rmq

import "math"

type Cleaner struct {
	connection Connection
}

func NewCleaner(connection Connection) *Cleaner {
	return &Cleaner{connection: connection}
}

// CleanExpired performs an on-demand cleanup of expired deliveries across all
// open queues. In contrast to a full scan it is cheap and bounded:
//
//   - Queues whose ready list is shorter than threshold are skipped without
//     any scan command, so small queues and idle systems cost nothing.
//   - Queues at or above threshold are scanned from the head (the oldest end
//     consumers read from) only. The scan inspects at most max deliveries and
//     stops at the first delivery which is still valid, so it never walks the
//     whole backlog and never touches unacked deliveries or consumer cursors.
//
// Non-positive threshold or max fall back to DefaultExpiryCleanThreshold and
// DefaultExpiryCleanMax. It returns the number of expired deliveries removed.
func (cleaner *Cleaner) CleanExpired(threshold, max int64) (removed int64, err error) {
	if threshold <= 0 {
		threshold = DefaultExpiryCleanThreshold
	}
	if max <= 0 {
		max = DefaultExpiryCleanMax
	}

	queueNames, err := cleaner.connection.GetOpenQueues()
	if err != nil {
		return 0, err
	}

	now := nowFunc()
	for _, queueName := range queueNames {
		queue, err := cleaner.connection.OpenQueue(queueName)
		if err != nil {
			return removed, err
		}

		count, err := queue.readyCount()
		if err != nil {
			return removed, err
		}
		if count < threshold {
			continue
		}

		n, err := queue.cleanExpiredHead(now, max)
		if err != nil {
			return removed, err
		}
		removed += n
	}

	return removed, nil
}

// Clean cleans the connection of the cleaner. This is useful to make sure no
// deliveries get lost. The main use case is if your consumers get restarted
// there will be unacked deliveries assigned to the connection. Once the
// heartbeat of that connection dies the cleaner can recognize that and remove
// those unacked deliveries back to the ready list. If there was no error it
// returns the number of deliveries which have been returned from unacked lists
// to ready lists across all cleaned connections and queues.
func (cleaner *Cleaner) Clean() (returned int64, err error) {
	connectionNames, err := cleaner.connection.getConnections()
	if err != nil {
		return 0, err
	}

	for _, connectionName := range connectionNames {
		hijackedConnection := cleaner.connection.hijackConnection(connectionName)
		switch err := hijackedConnection.checkHeartbeat(); err {
		case nil: // active connection
			continue
		case ErrorNotFound:
			n, err := cleanStaleConnection(hijackedConnection)
			if err != nil {
				return 0, err
			}
			returned += n
		default:
			return 0, err
		}
	}

	return returned, nil
}

func cleanStaleConnection(staleConnection Connection) (returned int64, err error) {
	queueNames, err := staleConnection.getConsumingQueues()
	if err != nil {
		return 0, err
	}

	for _, queueName := range queueNames {
		queue, err := staleConnection.OpenQueue(queueName)
		if err != nil {
			return 0, err
		}

		n, err := cleanQueue(queue)
		if err != nil {
			return 0, err
		}

		returned += n
	}

	if err := staleConnection.closeStaleConnection(); err != nil {
		return 0, err
	}

	// log.Printf("rmq cleaner cleaned connection %s", staleConnection)
	return returned, nil
}

func cleanQueue(queue Queue) (returned int64, err error) {
	returned, err = queue.ReturnUnacked(math.MaxInt64)
	if err != nil {
		return 0, err
	}
	if err := queue.closeInStaleConnection(); err != nil {
		return 0, err
	}
	// log.Printf("rmq cleaner cleaned queue %s %d", queue, returned)
	return returned, nil
}
