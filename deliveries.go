package rmq

type Deliveries []Delivery

func (deliveries Deliveries) Payloads() []string {
	payloads := make([]string, len(deliveries))
	for i, delivery := range deliveries {
		payloads[i] = delivery.Payload()
	}
	return payloads
}

// NOTE: The returned error map maps delivery indexes to errors. So if the
// error map is non empty you can use the indexes in the map to look up which
// of the deliveries ran into the corresponding error. See
// example/batch_consumer.

// functions with retry, see comments in delivery.go (recommended)

func (deliveries Deliveries) Ack() (errMap map[int]error) {
	if errMap, ok := deliveries.ackBatch(); ok {
		return errMap
	}
	return deliveries.each(Delivery.Ack)
}

func (deliveries Deliveries) Reject() (errMap map[int]error) {
	return deliveries.each(Delivery.Reject)
}

func (deliveries Deliveries) Push() (errMap map[int]error) {
	return deliveries.each(Delivery.Push)
}

// helper functions

func (deliveries Deliveries) each(
	f func(Delivery) error,
) (errMap map[int]error) {
	for i, delivery := range deliveries {
		if err := f(delivery); err != nil {
			if errMap == nil { // create error map lazily on demand
				errMap = map[int]error{}
			}
			errMap[i] = err
		}
	}
	return errMap
}

func (deliveries Deliveries) ackBatch() (errMap map[int]error, ok bool) {
	if len(deliveries) == 0 {
		return nil, true
	}

	first, ok := deliveries[0].(*redisDelivery)
	if !ok {
		return nil, false
	}
	client, ok := first.redisClient.(multiRemover)
	if !ok {
		return nil, false
	}

	payloads := make([]string, len(deliveries))
	for i, delivery := range deliveries {
		redisDelivery, ok := delivery.(*redisDelivery)
		if !ok || redisDelivery.unackedKey != first.unackedKey {
			return nil, false
		}
		payloads[i] = redisDelivery.payload
	}

	counts, err := client.LRemMulti(first.unackedKey, payloads)
	if err != nil || len(counts) != len(payloads) {
		return nil, false
	}

	for i, count := range counts {
		if count == 0 {
			if errMap == nil {
				errMap = map[int]error{}
			}
			errMap[i] = ErrorNotFound
		}
	}
	return errMap, true
}

type multiRemover interface {
	LRemMulti(key string, values []string) ([]int64, error)
}

type listMover interface {
	MoveListItem(source, destination, value string) (int64, error)
}
