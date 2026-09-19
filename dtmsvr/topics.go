package dtmsvr

import (
	"errors"

	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/client/dtmcli/logger"
)

const (
	topicsCat = "topics"
)

// TopicTransCompleted is the builtin topic. subscribers of this topic will
// receive an event when any type of global transaction reaches a final status
const TopicTransCompleted = "dtm_trans_completed"

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

// TransBranchResult the final result of a participated branch
type TransBranchResult struct {
	BranchID string `json:"branch_id"`
	Op       string `json:"op"`
	URL      string `json:"url"`
	Status   string `json:"status"`
}

// TransCompletedEvent the event posted to subscribers of TopicTransCompleted
// when a global transaction of any type reaches a final status
type TransCompletedEvent struct {
	Gid            string              `json:"gid"`
	TransType      string              `json:"trans_type"`
	Status         string              `json:"status"`
	RollbackReason string              `json:"rollback_reason,omitempty"`
	Branches       []TransBranchResult `json:"branches"`
}

func topic2urls(topic string) []string {
	urls := make([]string, len(topicsMap[topic].Subscribers))
	for k, subscriber := range topicsMap[topic].Subscribers {
		urls[k] = subscriber.URL
	}
	return urls
}

// notifyTransCompleted posts the completed event of a global transaction to
// the subscribers of TopicTransCompleted. timeout or failure will not affect
// the transaction, only an error log is recorded
func notifyTransCompleted(t *TransGlobal) {
	urls := topic2urls(TopicTransCompleted)
	if len(urls) == 0 {
		return
	}
	event := &TransCompletedEvent{
		Gid:            t.Gid,
		TransType:      t.TransType,
		Status:         t.Status,
		RollbackReason: t.RollbackReason,
	}
	for _, branch := range GetStore().FindBranches(t.Gid) {
		event.Branches = append(event.Branches, TransBranchResult{
			BranchID: branch.BranchID,
			Op:       branch.Op,
			URL:      branch.URL,
			Status:   branch.Status,
		})
	}
	for _, url := range urls {
		resp, err := dtmimp.GetRestyClient2(transHookTimeout).R().
			SetBody(event).
			SetHeader("Content-type", "application/json").
			Post(url)
		if err != nil {
			logger.Errorf("notify trans completed failed. topic: %s url: %s gid: %s err: %v",
				TopicTransCompleted, url, t.Gid, err)
			continue
		}
		if resp.IsError() {
			logger.Errorf("notify trans completed return error. topic: %s url: %s gid: %s status: %d body: %s",
				TopicTransCompleted, url, t.Gid, resp.StatusCode(), resp.String())
		}
	}
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
