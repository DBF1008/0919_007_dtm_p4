package test

import (
	"testing"

	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/dtmsvr"
	"github.com/dtm-labs/dtm/test/busi"
	"github.com/stretchr/testify/assert"
)

func TestTransHookCommit(t *testing.T) {
	saga := genSaga(dtmimp.GetFuncName(), false, false)
	saga.Hooks.BeforeCommit = busi.Busi + "/TransHook"
	saga.Hooks.AfterCommit = busi.Busi + "/TransHook"
	saga.Hooks.OnRollback = busi.Busi + "/TransHook"
	err := saga.Submit()
	assert.Nil(t, err)
	waitTransProcessed(saga.Gid)
	assert.Equal(t, StatusSucceed, getTransStatus(saga.Gid))

	before := busi.TransHookResult[saga.Gid+"/"+dtmsvr.HookBeforeCommit]
	assert.Equal(t, saga.Gid, before["gid"])
	assert.Equal(t, "saga", before["trans_type"])
	assert.Equal(t, StatusSucceed, before["status"])
	after := busi.TransHookResult[saga.Gid+"/"+dtmsvr.HookAfterCommit]
	assert.Equal(t, saga.Gid, after["gid"])
	assert.Equal(t, StatusSucceed, after["status"])
	_, ok := busi.TransHookResult[saga.Gid+"/"+dtmsvr.HookOnRollback]
	assert.False(t, ok)
}

func TestTransHookRollback(t *testing.T) {
	saga := genSaga(dtmimp.GetFuncName(), false, true)
	saga.Hooks.BeforeCommit = busi.Busi + "/TransHook"
	saga.Hooks.AfterCommit = busi.Busi + "/TransHook"
	saga.Hooks.OnRollback = busi.Busi + "/TransHook"
	err := saga.Submit()
	assert.Nil(t, err)
	waitTransProcessed(saga.Gid)
	assert.Equal(t, StatusFailed, getTransStatus(saga.Gid))

	rollback := busi.TransHookResult[saga.Gid+"/"+dtmsvr.HookOnRollback]
	assert.Equal(t, saga.Gid, rollback["gid"])
	assert.Equal(t, StatusFailed, rollback["status"])
	assert.NotEmpty(t, rollback["rollback_reason"])
	_, ok := busi.TransHookResult[saga.Gid+"/"+dtmsvr.HookBeforeCommit]
	assert.False(t, ok)
	_, ok = busi.TransHookResult[saga.Gid+"/"+dtmsvr.HookAfterCommit]
	assert.False(t, ok)
}

func TestTransHookErrorIgnored(t *testing.T) {
	saga := genSaga(dtmimp.GetFuncName(), false, false)
	saga.Hooks.BeforeCommit = "http://localhost:19998/unreachable" // connection refused
	saga.Hooks.AfterCommit = busi.Busi + "/TransHookError"         // returns error
	err := saga.Submit()
	assert.Nil(t, err)
	waitTransProcessed(saga.Gid)
	// hook failures should not affect the main transaction flow
	assert.Equal(t, StatusSucceed, getTransStatus(saga.Gid))
}

func TestTransCompletedTopic(t *testing.T) {
	topic := dtmsvr.TransCompletedTopic
	e2p(httpSubscribe(topic, busi.Busi+"/TransCompleted"))
	dtmsvr.CronUpdateTopicsMapOnce()
	defer func() {
		e2p(httpUnsubscribe(topic, busi.Busi+"/TransCompleted"))
		dtmsvr.CronUpdateTopicsMapOnce()
	}()

	// msg trans succeeds, subscribers receive the final status and branch results
	gid := dtmimp.GetFuncName() + "_msg"
	msg := genMsg(gid)
	err := msg.Submit()
	assert.Nil(t, err)
	waitTransProcessed(msg.Gid)
	assert.Equal(t, StatusSucceed, getTransStatus(msg.Gid))
	event := busi.TransCompletedResult[gid]
	assert.Equal(t, gid, event["gid"])
	assert.Equal(t, "msg", event["trans_type"])
	assert.Equal(t, StatusSucceed, event["status"])
	assert.Equal(t, 2, len(event["branches"].([]interface{})))

	// saga trans rolls back, subscribers receive the failed status and rollback reason
	gid2 := dtmimp.GetFuncName() + "_saga"
	saga := genSaga(gid2, false, true)
	err = saga.Submit()
	assert.Nil(t, err)
	waitTransProcessed(saga.Gid)
	assert.Equal(t, StatusFailed, getTransStatus(saga.Gid))
	event = busi.TransCompletedResult[gid2]
	assert.Equal(t, gid2, event["gid"])
	assert.Equal(t, "saga", event["trans_type"])
	assert.Equal(t, StatusFailed, event["status"])
	assert.NotEmpty(t, event["rollback_reason"])
	assert.Equal(t, 4, len(event["branches"].([]interface{})))
}
