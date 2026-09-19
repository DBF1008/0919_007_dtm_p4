package dtmsvr

import (
	"errors"
	"fmt"
	"time"

	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/client/dtmcli/logger"
)

const (
	topicsCat = "topics"
)

// TransCompletedTopic is the reserved topic name for global transaction completion events.
// subscribers of this topic will be notified with the final status of any type of
// global transaction and the execution results of its participated branches.
const TransCompletedTopic = "dtm_trans_completed"

var topicsMap = map[string]Topic{}

// Topic define topic info
type Topic struct {
	Name        string       `json:"k"`
	Subscribers []Subscriber `json:"v"`
	Version     uint64       `json:"version"`
}

// Subscriber define subscriber info
type Subscriber struct {
	URL    string `json:"url"`
	Remark string `json:"remark"`
}

// TransCompletedEvent is the payload posted to subscribers of TransCompletedTopic
// when a global transaction of any type reaches a final status (succeed/failed).
type TransCompletedEvent struct {
	Gid            string        `json:"gid"`
	TransType      string        `json:"trans_type"`
	Status         string        `json:"status"`
	RollbackReason string        `json:"rollback_reason,omitempty"`
	Result         string        `json:"result,omitempty"`
	FinishTime     *time.Time    `json:"finish_time,omitempty"`
	RollbackTime   *time.Time    `json:"rollback_time,omitempty"`
	Branches       []TransBranch `json:"branches"`
}

// notifyTransCompleted notifies subscribers of TransCompletedTopic about the final
// status of a finished global transaction and its branch execution results.
// notification timeout/failure does not affect the main transaction flow, failures are only logged.
func notifyTransCompleted(t *TransGlobal) {
	subscribers := topicsMap[TransCompletedTopic].Subscribers
	if len(subscribers) == 0 {
		return
	}
	event := &TransCompletedEvent{
		Gid:            t.Gid,
		TransType:      t.TransType,
		Status:         t.Status,
		RollbackReason: t.RollbackReason,
		Result:         t.Result,
		FinishTime:     t.FinishTime,
		RollbackTime:   t.RollbackTime,
		Branches:       GetStore().FindBranches(t.Gid),
	}
	for _, subscriber := range subscribers {
		resp, err := dtmimp.GetRestyClient2(transCallbackTimeout).R().
			SetHeader("Content-type", "application/json").
			SetBody(event).
			Post(subscriber.URL)
		if err == nil && resp.IsError() {
			err = fmt.Errorf("status code: %d body: %s", resp.StatusCode(), resp.String())
		}
		if err != nil {
			logger.Errorf("notify trans completed failed. gid: %s url: %s error: %v",
				t.Gid, subscriber.URL, err)
			continue
		}
		logger.Infof("notify trans completed ok. gid: %s url: %s", t.Gid, subscriber.URL)
	}
}

func topic2urls(topic string) []string {
	urls := make([]string, len(topicsMap[topic].Subscribers))
	for k, subscriber := range topicsMap[topic].Subscribers {
		urls[k] = subscriber.URL
	}
	return urls
}

// Subscribe subscribes topic, create topic if not exist
func Subscribe(topic, url, remark string) error {
	if topic == "" {
		return errors.New("empty topic")
	}
	if url == "" {
		return errors.New("empty url")
	}

	newSubscriber := Subscriber{
		URL:    url,
		Remark: remark,
	}
	kvs := GetStore().FindKV(topicsCat, topic)
	if len(kvs) == 0 {
		return GetStore().CreateKV(topicsCat, topic, dtmimp.MustMarshalString([]Subscriber{newSubscriber}))
	}

	subscribers := []Subscriber{}
	dtmimp.MustUnmarshalString(kvs[0].V, &subscribers)
	for _, subscriber := range subscribers {
		if subscriber.URL == url {
			return errors.New("this url exists")
		}
	}
	subscribers = append(subscribers, newSubscriber)
	kvs[0].V = dtmimp.MustMarshalString(subscribers)
	return GetStore().UpdateKV(&kvs[0])
}

// Unsubscribe unsubscribes the topic
func Unsubscribe(topic, url string) error {
	if topic == "" {
		return errors.New("empty topic")
	}
	if url == "" {
		return errors.New("empty url")
	}

	kvs := GetStore().FindKV(topicsCat, topic)
	if len(kvs) == 0 {
		return errors.New("no such a topic")
	}
	subscribers := []Subscriber{}
	dtmimp.MustUnmarshalString(kvs[0].V, &subscribers)
	if len(subscribers) == 0 {
		return errors.New("this topic is empty")
	}
	n := len(subscribers)
	for k, subscriber := range subscribers {
		if subscriber.URL == url {
			subscribers = append(subscribers[:k], subscribers[k+1:]...)
			break
		}
	}
	if len(subscribers) == n {
		return errors.New("no such an url ")
	}
	kvs[0].V = dtmimp.MustMarshalString(subscribers)
	return GetStore().UpdateKV(&kvs[0])
}

// updateTopicsMap updates the topicsMap variable, unsafe for concurrent
func updateTopicsMap() {
	kvs := GetStore().FindKV(topicsCat, "")
	for _, kv := range kvs {
		topic := topicsMap[kv.K]
		if topic.Version >= kv.Version {
			continue
		}
		newTopic := Topic{}
		newTopic.Name = kv.K
		newTopic.Version = kv.Version
		dtmimp.MustUnmarshalString(kv.V, &newTopic.Subscribers)
		topicsMap[kv.K] = newTopic
		logger.Infof("topic updated. old topic:%v new topic:%v", topicsMap[kv.K], newTopic)
	}
	logger.Debugf("all topic updated. topic:%v", topicsMap)
}
