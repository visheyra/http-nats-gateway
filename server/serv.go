package server

import (
	"encoding/json"
	"github.com/visheyra/http-nats-gateway/nats"
	"go.uber.org/zap"
	"io/ioutil"
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
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logger.Warnw("Bad body received, not forwarding")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	//Unpack json
	store := make(map[string]interface{})
	if err := json.Unmarshal(data, &store); err != nil {
		logger.Errorw("Can't decode json",
			"error", err.Error(),
		)
		w.WriteHeader(http.StatusBadRequest)
		return
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
