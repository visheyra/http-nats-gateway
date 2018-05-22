package nats

import (
	"encoding/json"
	"github.com/nats-io/go-nats"
	"go.uber.org/zap"
)

func Forward(forwardAddr, user, pass, topic string, data map[string]interface{}) {

	//Create logger
	l, err := zap.NewProduction()
	if err != nil {
		return
	}
	defer l.Sync()
	logger := l.Sugar()

	//
	// Connect to nats server
	//
	cnx, err := nats.Connect(forwardAddr, nats.UserInfo(user, pass))
	if err != nil {
		logger.Errorw("Can't establish connection to nats endpoint",
			"endpoint", forwardAddr,
			"error", err.Error(),
		)
		return
	}
	defer cnx.Close()

	payload, err := json.Marshal(data)
	if err != nil {
		logger.Errorw("Can't pack message",
			"endpoint", forwardAddr,
			"error", err.Error(),
		)
		return
	}

	if err := cnx.Publish(topic, payload); err != nil {
		logger.Errorw("Can't send data to endpoint",
			"error", err.Error(),
			"endpoint", forwardAddr,
			"topic", topic,
		)
	} else {
		logger.Debugw("Successfully sent data",
			"endpoint", forwardAddr,
			"topic", topic,
			"size", len(payload),
		)
	}
}
