package server

import (
	"encoding/json"
	"github.com/visheyra/http-nats-gateway/nats"
	"go.uber.org/zap"
	"net/http"
)

type handler struct {
	forwardAddr string
	user        string
	pass        string
	topic       string
}

func (h handler) forward(w http.ResponseWriter, r *http.Request) {

	//Create logger
	l, err := zap.NewProduction()
	if err != nil {
		return
	}
	defer l.Sync()
	logger := l.Sugar()

	defer r.Body.Close()

	//Test that body is not empty
	if r.Body == nil {
		logger.Warnw("Empty body received, not forwarding")
		w.WriteHeader(http.StatusBadRequest)
	}

	//Unpack json
	data := make(map[string]interface{})
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		logger.Errorw("Can't decode json",
			"error", err.Error(),
		)
	}

	nats.Forward(h.forwardAddr, h.user, h.pass, h.topic, data)
}

func StartServer(listen, forward, user, pass, topic string) {

	l, err := zap.NewProduction()
	if err != nil {
		return
	}
	defer l.Sync()
	logger := l.Sugar()

	h := handler{
		forwardAddr: forward,
		user:        user,
		pass:        pass,
		topic:       topic,
	}

	http.HandleFunc("/fwd", h.forward)
	if err := http.ListenAndServe(listen, nil); err != nil {
		logger.Fatalw("Can't start server",
			"listen adress", listen,
			"error", err.Error(),
		)
	}
}
