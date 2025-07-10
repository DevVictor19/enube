package importer

var (
	subscriptionSequence = 0
	subscriptions        map[string]int
	subscriptionValues   []any
)

func getSubscriptionSk(row []string) int {
	if subscriptions == nil {
		subscriptions = make(map[string]int)
	}

	subscriptionId := row[subscriptionIdIndex]
	description := row[subscriptionDescriptionIndex]

	existentSequence, ok := subscriptions[subscriptionId]
	if !ok {
		subscriptionSequence++
		subscriptions[subscriptionId] = subscriptionSequence

		subscriptionValues = append(
			subscriptionValues,
			subscriptionSequence,
			subscriptionId,
			description,
		)

		return subscriptionSequence
	}

	return existentSequence
}

func getSubscriptionStm() string {
	table := "dim_subscriptions"
	cols := []string{
		"subscription_sk",
		"subscription_id",
		"description",
	}
	totalVals := len(subscriptionValues)
	return buildBatchInsert(table, cols, totalVals)
}
