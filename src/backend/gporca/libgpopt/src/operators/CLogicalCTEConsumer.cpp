//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalCTEConsumer.cpp
//
//	@doc:
//		Implementation of CTE consumer operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalCTEConsumer.h"

#include "gpos/base.h"

#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CMaxCard.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalCTEProducer.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::CLogicalCTEConsumer
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalCTEConsumer::CLogicalCTEConsumer(CMemoryPool *mp)
	: CLogical(mp),
	  m_id(0),
	  m_pdrgpcr(nullptr),
	  m_pexprInlined(nullptr),
	  m_phmulcr(nullptr),
	  m_pcrsOutput(nullptr),
	  m_pruned(false)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::CLogicalCTEConsumer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalCTEConsumer::CLogicalCTEConsumer(CMemoryPool *mp, ULONG id,
										 CColRefArray *colref_array)
	: CLogical(mp),
	  m_id(id),
	  m_pdrgpcr(colref_array),
	  m_pexprInlined(nullptr),
	  m_phmulcr(nullptr),
	  m_pruned(false)
{
	GPOS_ASSERT(nullptr != colref_array);
	m_pcrsOutput = GPOS_NEW(mp) CColRefSet(mp, m_pdrgpcr);
	CreateInlinedExpr(mp);
	m_pcrsLocalUsed->Include(m_pdrgpcr);

	// map consumer columns to their positions in consumer output
	COptCtxt::PoctxtFromTLS()->Pcteinfo()->AddConsumerCols(id, m_pdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::~CLogicalCTEConsumer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalCTEConsumer::~CLogicalCTEConsumer()
{
	CRefCount::SafeRelease(m_pdrgpcr);
	CRefCount::SafeRelease(m_pexprInlined);
	CRefCount::SafeRelease(m_phmulcr);
	CRefCount::SafeRelease(m_pcrsOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::CreateInlinedExpr
//
//	@doc:
//		Create the inlined version of this consumer as well as the column mapping
//
//---------------------------------------------------------------------------
void
CLogicalCTEConsumer::CreateInlinedExpr(CMemoryPool *mp)
{
	CExpression *pexprProducer =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(nullptr != pexprProducer);
	// the actual definition of the CTE is the first child of the producer
	CExpression *pexprCTEDef = (*pexprProducer)[0];

	CLogicalCTEProducer *popProducer =
		CLogicalCTEProducer::PopConvert(pexprProducer->Pop());

	m_phmulcr = CUtils::PhmulcrMapping(mp, popProducer->Pdrgpcr(), m_pdrgpcr);
	m_pexprInlined = pexprCTEDef->PexprCopyWithRemappedColumns(
		mp, m_phmulcr, true /*must_exist*/);
}

void 
CLogicalCTEConsumer::ApplyInline() {
	// do nothing 
	if (!m_pruned) {
		return;
	}
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	CExpressionArray *pcexprs = pcteinfo->PexprCTEConsumer(m_id);
	ULONG cpexrssz = pcexprs->Size();
	UlongToColRefMap *pcmpulcr = pcteinfo->GetCTEConsumerMapping();

	for (ULONG ulPos = 0; ulPos < cpexrssz; ulPos++) {
		CExpression *cepxr = (*pcexprs)[ulPos];
		CLogicalCTEConsumer *pcoper = CLogicalCTEConsumer::PopConvert(cepxr->Pop());
		CColRefArray *colrefs = pcoper->Pdrgpcr();

		for (ULONG ulcrPos = 0; ulcrPos < colrefs->Size(); ulcrPos++) {
			CColRef *colref = (*colrefs)[ulcrPos];
			ULONG colid = colref->Id();
			colref->MarkAsUsed();

			CColRef *producer_colref = nullptr;
			producer_colref = pcmpulcr->Find(&colid);
			if (producer_colref) {
				producer_colref->MarkAsUsed();
			}
		}

		pcoper->MarkAsNotPruned();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalCTEConsumer::DeriveOutputColumns(CMemoryPool *,		  //mp,
										 CExpressionHandle &  //exprhdl
)
{
	// It's fine if we not prune the pcrsOutput
	m_pcrsOutput->AddRef();
	return m_pcrsOutput;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::DeriveNotNullColumns
//
//	@doc:
//		Derive not nullable output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalCTEConsumer::DeriveNotNullColumns(CMemoryPool *mp,
										  CExpressionHandle &  // exprhdl
) const
{
	CExpression *pexprProducer =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(nullptr != pexprProducer);

	// find producer's not null columns
	CColRefSet *pcrsProducerNotNull = pexprProducer->DeriveNotNullColumns();

	// map producer's not null columns to consumer's output columns
	CColRefSet *pcrsConsumerNotNull = CUtils::PcrsRemap(
		mp, pcrsProducerNotNull, m_phmulcr, true /*must_exist*/);
	GPOS_ASSERT(pcrsConsumerNotNull->Size() == pcrsProducerNotNull->Size());

	return pcrsConsumerNotNull;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalCTEConsumer::DeriveKeyCollection(CMemoryPool *,		  //mp,
										 CExpressionHandle &  //exprhdl
) const
{
	CExpression *pexpr =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(nullptr != pexpr);
	CKeyCollection *pkc = pexpr->DeriveKeyCollection();
	if (nullptr != pkc)
	{
		pkc->AddRef();
	}

	return pkc;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::DerivePartitionInfo
//
//	@doc:
//		Derive partition consumers
//
//---------------------------------------------------------------------------
CPartInfo *
CLogicalCTEConsumer::DerivePartitionInfo(CMemoryPool *,		  //mp,
										 CExpressionHandle &  //exprhdl
) const
{
	CPartInfo *ppartInfo = m_pexprInlined->DerivePartitionInfo();
	ppartInfo->AddRef();

	return ppartInfo;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalCTEConsumer::DeriveMaxCard(CMemoryPool *,		//mp,
								   CExpressionHandle &	//exprhdl
) const
{
	CExpression *pexpr =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(nullptr != pexpr);
	return pexpr->DeriveMaxCard();
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::DeriveJoinDepth
//
//	@doc:
//		Derive join depth
//
//---------------------------------------------------------------------------
ULONG
CLogicalCTEConsumer::DeriveJoinDepth(CMemoryPool *,		  //mp,
									 CExpressionHandle &  //exprhdl
) const
{
	CExpression *pexpr =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(nullptr != pexpr);
	return pexpr->DeriveJoinDepth();
}

// derive table descriptor
CTableDescriptorHashSet *
CLogicalCTEConsumer::DeriveTableDescriptor(CMemoryPool *,		//mp
										   CExpressionHandle &	//exprhdl
) const
{
	CExpression *pexpr =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(nullptr != pexpr);

	CTableDescriptorHashSet *table_descriptor_set =
		pexpr->DeriveTableDescriptor();
	table_descriptor_set->AddRef();
	return table_descriptor_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalCTEConsumer::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalCTEConsumer *popCTEConsumer = CLogicalCTEConsumer::PopConvert(pop);

	return m_id == popCTEConsumer->UlCTEId() &&
		   m_pdrgpcr->Equals(popCTEConsumer->Pdrgpcr());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalCTEConsumer::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(), m_id);
	ulHash = gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcr));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CLogicalCTEConsumer::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected function call of FInputOrderSensitive");
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalCTEConsumer::PopCopyWithRemappedColumns(
	CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	CColRefArray *colref_array = nullptr;
	CCTEInfo *cteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();

	if (must_exist)
	{
		UlongToColRefMap *consumer_mapping;
		ULONG colid;
		ULONG colref_size;

		colref_array = CUtils::PdrgpcrRemapAndCreate(mp, m_pdrgpcr, colref_mapping);

		colref_size = colref_array->Size();
		GPOS_ASSERT(colref_size == colref_array->Size());

		consumer_mapping = cteinfo->GetCTEConsumerMapping();
		for (ULONG ul = 0; ul < colref_size; ul++) {
			CColRef *old_colref = (*m_pdrgpcr)[ul];
			CColRef *new_colref = (*colref_array)[ul];
			
			colid = old_colref->Id();
			CColRef *p_colref = consumer_mapping->Find(&colid);
			if (nullptr == p_colref) {
				GPOS_RAISE(
					CException::ExmaInvalid, CException::ExmiInvalid,
					GPOS_WSZ_LIT(
						"Not found CTE consumer colid regsiterd in consumer mapping"));
			}
			
			consumer_mapping->Insert(
				GPOS_NEW(mp) ULONG(new_colref->Id()), p_colref);
		}
	}
	else
	{
		colref_array =
			CUtils::PdrgpcrRemap(mp, m_pdrgpcr, colref_mapping, must_exist);
	}

	CExpression *new_pexpr = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalCTEConsumer(mp, m_id, colref_array));

	cteinfo->AddCTEConsumer(new_pexpr);
	return new_pexpr->Pop();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalCTEConsumer::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfInlineCTEConsumer);
	(void) xform_set->ExchangeSet(CXform::ExfImplementCTEConsumer);
	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::DerivePropertyConstraint
//
//	@doc:
//		Derive constraint property
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogicalCTEConsumer::DerivePropertyConstraint(CMemoryPool *mp,
											  CExpressionHandle &  //exprhdl
) const
{
	CExpression *pexprProducer =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(nullptr != pexprProducer);
	CPropConstraint *ppc = pexprProducer->DerivePropertyConstraint();
	CColRefSetArray *pdrgpcrs = ppc->PdrgpcrsEquivClasses();
	CConstraint *pcnstr = ppc->Pcnstr();

	// remap producer columns to consumer columns
	CColRefSetArray *pdrgpcrsMapped = GPOS_NEW(mp) CColRefSetArray(mp);
	const ULONG length = pdrgpcrs->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRefSet *pcrs = (*pdrgpcrs)[ul];
		CColRefSet *pcrsMapped =
			CUtils::PcrsRemap(mp, pcrs, m_phmulcr, true /*must_exist*/);
		pdrgpcrsMapped->Append(pcrsMapped);
	}

	CConstraint *pcnstrMapped = nullptr;
	if (nullptr != pcnstr)
	{
		pcnstrMapped = pcnstr->PcnstrCopyWithRemappedColumns(
			mp, m_phmulcr, true /*must_exist*/);
	}

	return GPOS_NEW(mp) CPropConstraint(mp, pdrgpcrsMapped, pcnstrMapped);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::PstatsDerive
//
//	@doc:
//		Derive statistics based on cte producer
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalCTEConsumer::PstatsDerive(CMemoryPool *mp,
								  CExpressionHandle &,	//exprhdl,
								  IStatisticsArray *	// statistics_array
) const
{
	CExpression *pexprProducer =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(nullptr != pexprProducer);
	const IStatistics *stats = pexprProducer->Pstats();
	GPOS_ASSERT(nullptr != stats);

	// copy the stats with the remaped colids
	IStatistics *new_stats = stats->CopyStatsWithRemap(mp, m_phmulcr);

	return new_stats;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalCTEConsumer::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	os << m_id;
	os << "), Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
	os << "]";

	return os;
}

// EOF
