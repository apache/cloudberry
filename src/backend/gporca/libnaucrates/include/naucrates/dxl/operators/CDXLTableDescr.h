//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLTableDescr.h
//
//	@doc:
//		Class for representing table descriptors.
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLTableDescriptor_H
#define GPDXL_CDXLTableDescriptor_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLColDescr.h"
#include "naucrates/md/CMDName.h"
#include "naucrates/md/IMDId.h"

#define GPDXL_ACL_UNDEFINED (gpos::ulong_max)
// default value for m_assigned_query_id_for_target_rel - no assigned query for table descriptor
#define UNASSIGNED_QUERYID 0
// default value for m_security_quals_id - the scan has no security quals
#define NO_SECURITY_QUALS_ID 0

namespace gpdxl
{
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CDXLTableDescr
//
//	@doc:
//		Class for representing table descriptors in a DXL tablescan node.
//
//---------------------------------------------------------------------------
class CDXLTableDescr : public CRefCount
{
private:
	// id and version information for the table
	IMDId *m_mdid;

	// table name
	CMDName *m_mdname;

	// list of column descriptors
	CDXLColDescrArray *m_dxl_column_descr_array;

	// id of user the table needs to be accessed with
	ULONG m_execute_as_user_id;

	// lock mode from the parser
	INT m_lockmode;

	// acl mode from the parser
	ULONG m_acl_mode;

	// identifier of query to which current table belongs.
	// This field is used for assigning current table entry with
	// target one within DML operation. If descriptor doesn't point
	// to the target (result) relation it has value UNASSIGNED_QUERYID
	ULONG m_assigned_query_id_for_target_rel;

	// identifier of the security quals (row level security policies) of
	// the range table entry this table descriptor was created for; the
	// translator from the query assigns it, and the translator to the plan
	// looks the quals up with it. NO_SECURITY_QUALS_ID if there are none.
	// Several range table entries can refer to the same relation with
	// different security quals, so the relation's mdid can't be used.
	ULONG m_security_quals_id;

	void SerializeMDId(CXMLSerializer *xml_serializer) const;

public:
	CDXLTableDescr(const CDXLTableDescr &) = delete;

	// ctor/dtor
	CDXLTableDescr(CMemoryPool *mp, IMDId *mdid, CMDName *mdname,
				   ULONG ulExecuteAsUser, int lockmode, ULONG acl_mode,
				   ULONG assigned_query_id_for_target_rel = UNASSIGNED_QUERYID,
				   ULONG security_quals_id = NO_SECURITY_QUALS_ID);

	~CDXLTableDescr() override;

	// setters
	void SetColumnDescriptors(CDXLColDescrArray *dxl_column_descr_array);

	void AddColumnDescr(CDXLColDescr *pdxlcd);

	// table name
	const CMDName *MdName() const;

	// table mdid
	IMDId *MDId() const;

	// table arity
	ULONG Arity() const;

	// user id
	ULONG GetExecuteAsUserId() const;

	// lock mode
	INT LockMode() const;

	// acl mode
	ULONG GetAclMode() const;

	// get the column descriptor at the given position
	const CDXLColDescr *GetColumnDescrAt(ULONG idx) const;

	// serialize to dxl format
	void SerializeToDXL(CXMLSerializer *xml_serializer) const;

	// get assigned query id for target relation
	ULONG GetAssignedQueryIdForTargetRel() const;

	// get the identifier of the security quals of the range table entry
	ULONG
	GetSecurityQualsId() const
	{
		return m_security_quals_id;
	}
};
}  // namespace gpdxl


#endif	// !GPDXL_CDXLTableDescriptor_H

// EOF
