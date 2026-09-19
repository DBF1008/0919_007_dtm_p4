/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package test

import (
	"testing"

	"github.com/dtm-labs/dtm/client/dtmcli"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/dtmsvr"
	"github.com/dtm-labs/dtm/test/busi"
	"github.com/stretchr/testify/assert"
)

func TestTransHookSucceed(t *testing.T) {
	saga := genSaga(dtmimp.GetFuncName(), false, false)
	saga.Hooks = dtmcli.TransHooks{
		BeforeCommit: busi.Busi + "/TransHook",
		AfterCommit:  busi.Busi + "/TransHook",
	}
	err := saga.Submit()
	assert.Nil(t, err)
	waitTransProcessed(saga.Gid)
	assert.Equal(t, StatusSucceed, getTransStatus(saga.Gid))
	bc := busi.TransHookResults[dtmimp.HookBeforeCommit]
	assert.Equal(t, saga.Gid, bc["gid"])
	assert.Equal(t, dtmimp.HookBeforeCommit, bc["hook"])
	assert.Equal(t, StatusSucceed, bc["status"])
	ac := busi.TransHookResults[dtmimp.HookAfterCommit]
	assert.Equal(t, saga.Gid, ac["gid"])
	assert.Equal(t, dtmimp.HookAfterCommit, ac["hook"])
	assert.Equal(t, StatusSucceed, ac["status"])
}

func TestTransHookRollback(t *testing.T) {
	saga := genSaga(dtmimp.GetFuncName(), false, true)
	saga.Hooks = dtmcli.TransHooks{
		OnRollback: busi.Busi + "/TransHook",
	}
	err := saga.Submit()
	assert.Nil(t, err)
	waitTransProcessed(saga.Gid)
	assert.Equal(t, StatusFailed, getTransStatus(saga.Gid))
	rb := busi.TransHookResults[dtmimp.HookOnRollback]
	assert.Equal(t, saga.Gid, rb["gid"])
	assert.Equal(t, dtmimp.HookOnRollback, rb["hook"])
	assert.Equal(t, StatusFailed, rb["status"])
	assert.NotEmpty(t, rb["rollback_reason"])
}

func TestTransHookError(t *testing.T) {
	saga := genSaga(dtmimp.GetFuncName(), false, false)
	saga.Hooks = dtmcli.TransHooks{
		BeforeCommit: "http://localhost:9/invalid", // hook failure should not affect the trans
		AfterCommit:  busi.Busi + "/TransHook",
	}
	err := saga.Submit()
	assert.Nil(t, err)
	waitTransProcessed(saga.Gid)
	assert.Equal(t, StatusSucceed, getTransStatus(saga.Gid))
	ac := busi.TransHookResults[dtmimp.HookAfterCommit]
	assert.Equal(t, saga.Gid, ac["gid"])
}

func TestTopicTransCompletedEvent(t *testing.T) {
	e2p(httpSubscribe(dtmsvr.TopicTransCompleted, busi.Busi+"/TopicEvent"))
	dtmsvr.CronUpdateTopicsMapOnce()
	defer func() {
		e2p(httpUnsubscribe(dtmsvr.TopicTransCompleted, busi.Busi+"/TopicEvent"))
		dtmsvr.CronUpdateTopicsMapOnce()
	}()

	// any type of trans reaches final status will notify the subscribers
	saga := genSaga(dtmimp.GetFuncName()+"_1", false, false)
	err := saga.Submit()
	assert.Nil(t, err)
	waitTransProcessed(saga.Gid)
	assert.Equal(t, StatusSucceed, getTransStatus(saga.Gid))
	event := busi.TopicEventResult
	assert.Equal(t, saga.Gid, event["gid"])
	assert.Equal(t, "saga", event["trans_type"])
	assert.Equal(t, StatusSucceed, event["status"])
	branches, ok := event["branches"].([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 4, len(branches))

	// failed trans also notifies the subscribers with final status and rollback reason
	saga2 := genSaga(dtmimp.GetFuncName()+"_2", false, true)
	err = saga2.Submit()
	assert.Nil(t, err)
	waitTransProcessed(saga2.Gid)
	assert.Equal(t, StatusFailed, getTransStatus(saga2.Gid))
	event = busi.TopicEventResult
	assert.Equal(t, saga2.Gid, event["gid"])
	assert.Equal(t, StatusFailed, event["status"])
	assert.NotEmpty(t, event["rollback_reason"])
	branches, ok = event["branches"].([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 4, len(branches))
}
