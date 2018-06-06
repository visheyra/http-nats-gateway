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

	logger.Debugw("Received an existing body",
		"data", string(data[:]),
	)

	var storeArray []json.RawMessage

	//Unpack json
	if err := json.Unmarshal(data, &storeArray); err != nil {

		logger.Debugw("Can't decode value as array, decoding as object",
			"error", err.Error(),
			"data", string(data[:]),
		)

		storeObject := make(map[string]interface{})
		err = json.Unmarshal(data, &storeObject)
		if err != nil {
			logger.Errorw("Can't decode data as json, skipping",
				"data", string(data[:]),
				"error", err.Error(),
			)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		x, err := json.Marshal(storeObject)
		if err != nil {
			logger.Warnw("Error while unpacking json",
				"error", err.Error(),
			)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		nats.Forward(h.forwardAddr, h.user, h.pass, h.topic, x)
		w.WriteHeader(http.StatusOK)
		return
	}

	logger.Debugw("found array item",
		"length", len(storeArray),
	)

	for _, j := range storeArray {
		x, err := j.MarshalJSON()

		if err != nil {
			logger.Warnw("Got issue while forwarding",
				"error", err.Error())
			continue
		}
		nats.Forward(h.forwardAddr, h.user, h.pass, h.topic, x)
	}
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
