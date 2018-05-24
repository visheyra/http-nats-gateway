package nats

import (
	"github.com/nats-io/go-nats"
	"go.uber.org/zap"
)

func Forward(forwardAddr, user, pass, topic string, data []byte) {

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

	if err := cnx.Publish(topic, data); err != nil {
		logger.Errorw("Can't send data to endpoint",
			"error", err.Error(),
			"endpoint", forwardAddr,
			"topic", topic,
		)
	} else {
		logger.Debugw("Successfully sent data",
			"endpoint", forwardAddr,
			"topic", topic,
			"size", len(data),
		)
	}
}
