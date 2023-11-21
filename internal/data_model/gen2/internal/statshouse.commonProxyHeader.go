// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Code generated by vktl/cmd/tlgen2; DO NOT EDIT.
package internal

import (
	"github.com/vkcom/statshouse/internal/vkgo/basictl"
)

var _ = basictl.NatWrite

type StatshouseCommonProxyHeader struct {
	// IngressProxy (TrueType) // Conditional: nat_fields_mask.31
	// AgentEnvStaging (TrueType) // Conditional: nat_fields_mask.30
	ShardReplica      int32
	ShardReplicaTotal int32
	AgentIp           [4]int32
	HostName          string
	ComponentTag      int32
	BuildArch         int32
}

func (StatshouseCommonProxyHeader) TLName() string { return "statshouse.commonProxyHeader" }
func (StatshouseCommonProxyHeader) TLTag() uint32  { return 0x6c803d07 }

func (item *StatshouseCommonProxyHeader) SetIngressProxy(v bool, nat_fields_mask *uint32) {
	if nat_fields_mask != nil {
		if v {
			*nat_fields_mask |= 1 << 31
		} else {
			*nat_fields_mask &^= 1 << 31
		}
	}
}
func (item StatshouseCommonProxyHeader) IsSetIngressProxy(nat_fields_mask uint32) bool {
	return nat_fields_mask&(1<<31) != 0
}

func (item *StatshouseCommonProxyHeader) SetAgentEnvStaging(v bool, nat_fields_mask *uint32) {
	if nat_fields_mask != nil {
		if v {
			*nat_fields_mask |= 1 << 30
		} else {
			*nat_fields_mask &^= 1 << 30
		}
	}
}
func (item StatshouseCommonProxyHeader) IsSetAgentEnvStaging(nat_fields_mask uint32) bool {
	return nat_fields_mask&(1<<30) != 0
}

func (item *StatshouseCommonProxyHeader) Reset() {
	item.ShardReplica = 0
	item.ShardReplicaTotal = 0
	TupleInt4Reset(&item.AgentIp)
	item.HostName = ""
	item.ComponentTag = 0
	item.BuildArch = 0
}

func (item *StatshouseCommonProxyHeader) Read(w []byte, nat_fields_mask uint32) (_ []byte, err error) {
	if w, err = basictl.IntRead(w, &item.ShardReplica); err != nil {
		return w, err
	}
	if w, err = basictl.IntRead(w, &item.ShardReplicaTotal); err != nil {
		return w, err
	}
	if w, err = TupleInt4Read(w, &item.AgentIp); err != nil {
		return w, err
	}
	if w, err = basictl.StringRead(w, &item.HostName); err != nil {
		return w, err
	}
	if w, err = basictl.IntRead(w, &item.ComponentTag); err != nil {
		return w, err
	}
	return basictl.IntRead(w, &item.BuildArch)
}

func (item *StatshouseCommonProxyHeader) Write(w []byte, nat_fields_mask uint32) (_ []byte, err error) {
	w = basictl.IntWrite(w, item.ShardReplica)
	w = basictl.IntWrite(w, item.ShardReplicaTotal)
	if w, err = TupleInt4Write(w, &item.AgentIp); err != nil {
		return w, err
	}
	if w, err = basictl.StringWrite(w, item.HostName); err != nil {
		return w, err
	}
	w = basictl.IntWrite(w, item.ComponentTag)
	return basictl.IntWrite(w, item.BuildArch), nil
}

func (item *StatshouseCommonProxyHeader) ReadBoxed(w []byte, nat_fields_mask uint32) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x6c803d07); err != nil {
		return w, err
	}
	return item.Read(w, nat_fields_mask)
}

func (item *StatshouseCommonProxyHeader) WriteBoxed(w []byte, nat_fields_mask uint32) ([]byte, error) {
	w = basictl.NatWrite(w, 0x6c803d07)
	return item.Write(w, nat_fields_mask)
}

func StatshouseCommonProxyHeader__ReadJSON(item *StatshouseCommonProxyHeader, j interface{}, nat_fields_mask uint32) error {
	return item.readJSON(j, nat_fields_mask)
}
func (item *StatshouseCommonProxyHeader) readJSON(j interface{}, nat_fields_mask uint32) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("statshouse.commonProxyHeader", "expected json object")
	}
	_jIngressProxy := _jm["ingress_proxy"]
	delete(_jm, "ingress_proxy")
	_jAgentEnvStaging := _jm["agent_env_staging"]
	delete(_jm, "agent_env_staging")
	_jShardReplica := _jm["shard_replica"]
	delete(_jm, "shard_replica")
	if err := JsonReadInt32(_jShardReplica, &item.ShardReplica); err != nil {
		return err
	}
	_jShardReplicaTotal := _jm["shard_replica_total"]
	delete(_jm, "shard_replica_total")
	if err := JsonReadInt32(_jShardReplicaTotal, &item.ShardReplicaTotal); err != nil {
		return err
	}
	_jAgentIp := _jm["agent_ip"]
	delete(_jm, "agent_ip")
	_jHostName := _jm["host_name"]
	delete(_jm, "host_name")
	if err := JsonReadString(_jHostName, &item.HostName); err != nil {
		return err
	}
	_jComponentTag := _jm["component_tag"]
	delete(_jm, "component_tag")
	if err := JsonReadInt32(_jComponentTag, &item.ComponentTag); err != nil {
		return err
	}
	_jBuildArch := _jm["build_arch"]
	delete(_jm, "build_arch")
	if err := JsonReadInt32(_jBuildArch, &item.BuildArch); err != nil {
		return err
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("statshouse.commonProxyHeader", k)
	}
	if _jIngressProxy != nil {
		return ErrorInvalidJSON("statshouse.commonProxyHeader", "implicit true field 'ingress_proxy' cannot be defined, set fieldmask instead")
	}
	if _jAgentEnvStaging != nil {
		return ErrorInvalidJSON("statshouse.commonProxyHeader", "implicit true field 'agent_env_staging' cannot be defined, set fieldmask instead")
	}
	if err := TupleInt4ReadJSON(_jAgentIp, &item.AgentIp); err != nil {
		return err
	}
	return nil
}

func (item *StatshouseCommonProxyHeader) WriteJSON(w []byte, nat_fields_mask uint32) (_ []byte, err error) {
	w = append(w, '{')
	if item.ShardReplica != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"shard_replica":`...)
		w = basictl.JSONWriteInt32(w, item.ShardReplica)
	}
	if item.ShardReplicaTotal != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"shard_replica_total":`...)
		w = basictl.JSONWriteInt32(w, item.ShardReplicaTotal)
	}
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"agent_ip":`...)
	if w, err = TupleInt4WriteJSON(w, &item.AgentIp); err != nil {
		return w, err
	}
	if len(item.HostName) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"host_name":`...)
		w = basictl.JSONWriteString(w, item.HostName)
	}
	if item.ComponentTag != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"component_tag":`...)
		w = basictl.JSONWriteInt32(w, item.ComponentTag)
	}
	if item.BuildArch != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"build_arch":`...)
		w = basictl.JSONWriteInt32(w, item.BuildArch)
	}
	return append(w, '}'), nil
}

type StatshouseCommonProxyHeaderBytes struct {
	// IngressProxy (TrueType) // Conditional: nat_fields_mask.31
	// AgentEnvStaging (TrueType) // Conditional: nat_fields_mask.30
	ShardReplica      int32
	ShardReplicaTotal int32
	AgentIp           [4]int32
	HostName          []byte
	ComponentTag      int32
	BuildArch         int32
}

func (StatshouseCommonProxyHeaderBytes) TLName() string { return "statshouse.commonProxyHeader" }
func (StatshouseCommonProxyHeaderBytes) TLTag() uint32  { return 0x6c803d07 }

func (item *StatshouseCommonProxyHeaderBytes) SetIngressProxy(v bool, nat_fields_mask *uint32) {
	if nat_fields_mask != nil {
		if v {
			*nat_fields_mask |= 1 << 31
		} else {
			*nat_fields_mask &^= 1 << 31
		}
	}
}
func (item StatshouseCommonProxyHeaderBytes) IsSetIngressProxy(nat_fields_mask uint32) bool {
	return nat_fields_mask&(1<<31) != 0
}

func (item *StatshouseCommonProxyHeaderBytes) SetAgentEnvStaging(v bool, nat_fields_mask *uint32) {
	if nat_fields_mask != nil {
		if v {
			*nat_fields_mask |= 1 << 30
		} else {
			*nat_fields_mask &^= 1 << 30
		}
	}
}
func (item StatshouseCommonProxyHeaderBytes) IsSetAgentEnvStaging(nat_fields_mask uint32) bool {
	return nat_fields_mask&(1<<30) != 0
}

func (item *StatshouseCommonProxyHeaderBytes) Reset() {
	item.ShardReplica = 0
	item.ShardReplicaTotal = 0
	TupleInt4Reset(&item.AgentIp)
	item.HostName = item.HostName[:0]
	item.ComponentTag = 0
	item.BuildArch = 0
}

func (item *StatshouseCommonProxyHeaderBytes) Read(w []byte, nat_fields_mask uint32) (_ []byte, err error) {
	if w, err = basictl.IntRead(w, &item.ShardReplica); err != nil {
		return w, err
	}
	if w, err = basictl.IntRead(w, &item.ShardReplicaTotal); err != nil {
		return w, err
	}
	if w, err = TupleInt4Read(w, &item.AgentIp); err != nil {
		return w, err
	}
	if w, err = basictl.StringReadBytes(w, &item.HostName); err != nil {
		return w, err
	}
	if w, err = basictl.IntRead(w, &item.ComponentTag); err != nil {
		return w, err
	}
	return basictl.IntRead(w, &item.BuildArch)
}

func (item *StatshouseCommonProxyHeaderBytes) Write(w []byte, nat_fields_mask uint32) (_ []byte, err error) {
	w = basictl.IntWrite(w, item.ShardReplica)
	w = basictl.IntWrite(w, item.ShardReplicaTotal)
	if w, err = TupleInt4Write(w, &item.AgentIp); err != nil {
		return w, err
	}
	if w, err = basictl.StringWriteBytes(w, item.HostName); err != nil {
		return w, err
	}
	w = basictl.IntWrite(w, item.ComponentTag)
	return basictl.IntWrite(w, item.BuildArch), nil
}

func (item *StatshouseCommonProxyHeaderBytes) ReadBoxed(w []byte, nat_fields_mask uint32) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x6c803d07); err != nil {
		return w, err
	}
	return item.Read(w, nat_fields_mask)
}

func (item *StatshouseCommonProxyHeaderBytes) WriteBoxed(w []byte, nat_fields_mask uint32) ([]byte, error) {
	w = basictl.NatWrite(w, 0x6c803d07)
	return item.Write(w, nat_fields_mask)
}

func StatshouseCommonProxyHeaderBytes__ReadJSON(item *StatshouseCommonProxyHeaderBytes, j interface{}, nat_fields_mask uint32) error {
	return item.readJSON(j, nat_fields_mask)
}
func (item *StatshouseCommonProxyHeaderBytes) readJSON(j interface{}, nat_fields_mask uint32) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("statshouse.commonProxyHeader", "expected json object")
	}
	_jIngressProxy := _jm["ingress_proxy"]
	delete(_jm, "ingress_proxy")
	_jAgentEnvStaging := _jm["agent_env_staging"]
	delete(_jm, "agent_env_staging")
	_jShardReplica := _jm["shard_replica"]
	delete(_jm, "shard_replica")
	if err := JsonReadInt32(_jShardReplica, &item.ShardReplica); err != nil {
		return err
	}
	_jShardReplicaTotal := _jm["shard_replica_total"]
	delete(_jm, "shard_replica_total")
	if err := JsonReadInt32(_jShardReplicaTotal, &item.ShardReplicaTotal); err != nil {
		return err
	}
	_jAgentIp := _jm["agent_ip"]
	delete(_jm, "agent_ip")
	_jHostName := _jm["host_name"]
	delete(_jm, "host_name")
	if err := JsonReadStringBytes(_jHostName, &item.HostName); err != nil {
		return err
	}
	_jComponentTag := _jm["component_tag"]
	delete(_jm, "component_tag")
	if err := JsonReadInt32(_jComponentTag, &item.ComponentTag); err != nil {
		return err
	}
	_jBuildArch := _jm["build_arch"]
	delete(_jm, "build_arch")
	if err := JsonReadInt32(_jBuildArch, &item.BuildArch); err != nil {
		return err
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("statshouse.commonProxyHeader", k)
	}
	if _jIngressProxy != nil {
		return ErrorInvalidJSON("statshouse.commonProxyHeader", "implicit true field 'ingress_proxy' cannot be defined, set fieldmask instead")
	}
	if _jAgentEnvStaging != nil {
		return ErrorInvalidJSON("statshouse.commonProxyHeader", "implicit true field 'agent_env_staging' cannot be defined, set fieldmask instead")
	}
	if err := TupleInt4ReadJSON(_jAgentIp, &item.AgentIp); err != nil {
		return err
	}
	return nil
}

func (item *StatshouseCommonProxyHeaderBytes) WriteJSON(w []byte, nat_fields_mask uint32) (_ []byte, err error) {
	w = append(w, '{')
	if item.ShardReplica != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"shard_replica":`...)
		w = basictl.JSONWriteInt32(w, item.ShardReplica)
	}
	if item.ShardReplicaTotal != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"shard_replica_total":`...)
		w = basictl.JSONWriteInt32(w, item.ShardReplicaTotal)
	}
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"agent_ip":`...)
	if w, err = TupleInt4WriteJSON(w, &item.AgentIp); err != nil {
		return w, err
	}
	if len(item.HostName) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"host_name":`...)
		w = basictl.JSONWriteStringBytes(w, item.HostName)
	}
	if item.ComponentTag != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"component_tag":`...)
		w = basictl.JSONWriteInt32(w, item.ComponentTag)
	}
	if item.BuildArch != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"build_arch":`...)
		w = basictl.JSONWriteInt32(w, item.BuildArch)
	}
	return append(w, '}'), nil
}
