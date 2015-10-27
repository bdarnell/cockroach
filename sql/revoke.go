// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql

import (
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
)

// Revoke removes privileges from users.
// Current status:
// - Target: single database or table.
// TODO(marc): open questions:
// - should we have root always allowed and not present in the permissions list?
// - should we make users case-insensitive?
// Privileges: GRANT on database/table.
//   Notes: postgres requires the object owner.
//          mysql requires the "grant option" and the same privileges, and sometimes superuser.
func (p *planner) Revoke(n *parser.Revoke) (planNode, error) {
	descriptor, err := p.getDescriptorFromTargetList(n.Targets)
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilege(descriptor, privilege.GRANT); err != nil {
		return nil, err
	}

	for _, grantee := range n.Grantees {
		descriptor.GetPrivileges().Revoke(grantee, n.Privileges)
	}

	if err := descriptor.Validate(); err != nil {
		return nil, err
	}

	if tableDesc, ok := descriptor.(*TableDescriptor); ok {
		// TODO(pmattis): This is a hack. Remove when schema change operations work
		// properly.
		p.hackNoteSchemaChange(tableDesc)
	}

	// Now update the descriptor.
	// TODO(marc): do this inside a transaction. This will be needed
	// when modifying multiple descriptors in the same op.
	descKey := MakeDescMetadataKey(descriptor.GetID())
	if err := p.txn.Put(descKey, descriptor); err != nil {
		return nil, err
	}

	return &valuesNode{}, nil
}
