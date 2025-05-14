//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalCTEProducer.h
//
//	@doc:
//		Logical CTE producer operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalCTEProducer_H
#define GPOPT_CLogicalCTEProducer_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogical.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalCTEProducer
//
//	@doc:
//		CTE producer operator
//
//---------------------------------------------------------------------------
class CLogicalCTEProducer : public CLogical
{
private:
	// cte identifier
	ULONG m_id;

	// cte columns
	CColRefArray *m_pdrgpcr;

	// output columns, same as cte columns but in CColRefSet
	CColRefSet *m_pcrsOutput;

	// can be pruned
	BOOL m_cbp;

	// used mask
	BOOL *m_umask;

public:
	CLogicalCTEProducer(const CLogicalCTEProducer &) = delete;

	// ctor
	explicit CLogicalCTEProducer(CMemoryPool *mp);

	// ctor
	CLogicalCTEProducer(CMemoryPool *mp, ULONG id, CColRefArray *colref_array,
		BOOL canbepruned = false);

	// dtor
	~CLogicalCTEProducer() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopLogicalCTEProducer;
	}

	const CHAR *
	SzId() const override
	{
		return "CLogicalCTEProducer";
	}

	// cte identifier
	ULONG
	UlCTEId() const
	{
		return m_id;
	}

	void
	MarkALLUsed() {
		ULONG colref_size = m_pdrgpcr->Size();
		for (ULONG index = 0; index < colref_size; index++) {
			CColRef *col_ref = (*m_pdrgpcr)[index];
			col_ref->MarkAsUsed();
		}
		m_cbp = false;
	}


	BOOL
	CanBePruned() const
	{
		return m_cbp;
	}

	BOOL *
	UsedMask() const
	{
		return m_umask;
	}

	// cte columns
	CColRefArray *
	Pdrgpcr() const
	{
		return m_pdrgpcr;
	}

	// cte columns in CColRefSet
	CColRefSet *
	DeriveOutputColumns() const
	{
		return m_pcrsOutput;
	}

	// Recalculate the output columns
	void RecalOutputColumns(BOOL *umask, ULONG sz);

	// operator specific hash function
	ULONG HashValue() const override;

	// match function
	BOOL Matches(COperator *pop) const override;

	// sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const override
	{
		return false;
	}

	// return a copy of the operator with remapped columns
	COperator *PopCopyWithRemappedColumns(CMemoryPool *mp,
										  UlongToColRefMap *colref_mapping,
										  BOOL must_exist) override;

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	CColRefSet *DeriveOutputColumns(CMemoryPool *mp,
									CExpressionHandle &exprhdl) override;

	// dervive keys
	CKeyCollection *DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// derive max card
	CMaxCard DeriveMaxCard(CMemoryPool *mp,
						   CExpressionHandle &exprhdl) const override;

	// derive not nullable output columns
	CColRefSet *DeriveNotNullColumns(CMemoryPool *mp,
									 CExpressionHandle &exprhdl) const override;

	// derive constraint property
	CPropConstraint *
	DerivePropertyConstraint(CMemoryPool *mp,
							 CExpressionHandle &exprhdl) const override
	{
		return PpcDeriveConstraintRestrict(mp, exprhdl, m_pcrsOutput);
	}

	// derive partition consumer info
	CPartInfo *
	DerivePartitionInfo(CMemoryPool *,	// mp,
						CExpressionHandle &exprhdl) const override
	{
		return PpartinfoPassThruOuter(exprhdl);
	}

	CTableDescriptorHashSet *DeriveTableDescriptor(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;
	// compute required stats columns of the n-th child
	CColRefSet *
	PcrsStat(CMemoryPool *,		   // mp
			 CExpressionHandle &,  // exprhdl
			 CColRefSet *pcrsInput,
			 ULONG	// child_index
	) const override
	{
		return PcrsStatsPassThru(pcrsInput);
	}

	// derive statistics
	IStatistics *
	PstatsDerive(CMemoryPool *,	 //mp,
				 CExpressionHandle &exprhdl,
				 IStatisticsArray *	 //stats_ctxt
	) const override
	{
		return PstatsPassThruOuter(exprhdl);
	}

	// stat promise
	EStatPromise
	Esp(CExpressionHandle &) const override
	{
		return CLogical::EspHigh;
	}

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	CXformSet *PxfsCandidates(CMemoryPool *mp) const override;

	//-------------------------------------------------------------------------------------

	// conversion function
	static CLogicalCTEProducer *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopLogicalCTEProducer == pop->Eopid());

		return dynamic_cast<CLogicalCTEProducer *>(pop);
	}

	// debug print
	IOstream &OsPrint(IOstream &) const override;

};	// class CLogicalCTEProducer

}  // namespace gpopt

#endif	// !GPOPT_CLogicalCTEProducer_H

// EOF
