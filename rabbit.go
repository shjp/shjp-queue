package queue

import core "github.com/shjp/shjp-core"

func composeQueueName(origin string, intent core.Intent) string {
	return origin + ":" + string(intent)
}

func composeTopicExchangePattern(intent core.Intent) string {
	return string(intent) + ".#"
}
