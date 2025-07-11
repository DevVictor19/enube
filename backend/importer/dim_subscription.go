package importer

import "database/sql"

var (
	subscriptionSequence = 0
	subscriptions        map[string]int
	subscriptionValues   []any
)

func getSubscriptionSk(row []string) sql.NullInt32 {
	subscriptionId := row[subscriptionIdIndex]
	description := row[subscriptionDescriptionIndex]

	if subscriptionId == "" {
		return sql.NullInt32{
			Valid: false,
		}
	}

	if subscriptions == nil {
		subscriptions = make(map[string]int)
	}

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

		return sql.NullInt32{
			Valid: true,
			Int32: int32(subscriptionSequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
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

func resetSubscriptionValues() {
	subscriptionValues = nil
}
