/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * pax_table_cluster.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/access/pax_table_cluster.cc
 *
 *-------------------------------------------------------------------------
 */

#include "access/pax_table_cluster.h"

#include "comm/cbdb_api.h"

#include <map>

#include "access/paxc_rel_options.h"
#include "catalog/pax_catalog.h"
#include "clustering/clustering.h"
#include "clustering/index_clustering.h"
#include "clustering/lexical_clustering.h"
#include "clustering/pax_clustering_reader.h"
#include "clustering/pax_clustering_writer.h"
#include "clustering/zorder_clustering.h"
#include "storage/micro_partition_iterator.h"
#include "storage/micro_partition_metadata.h"

#define CLUSTER_SORT_MEMORY 128000

static const char *pg_less_operator = "<";

namespace pax {
void DeleteClusteringFiles(
    Relation rel, Snapshot snapshot,
    std::shared_ptr<IteratorBase<MicroPartitionMetadata>> iter) {
  while (iter->HasNext()) {
    auto it = iter->Next();
    cbdb::DeleteMicroPartitionEntry(RelationGetRelid(rel), snapshot,
                                    it.GetMicroPartitionId());
  }
  // TODO(gongxun): mark files deleted or delete files directly?
}

// TODO(gongxun): should check more operators
static Oid OperatorGetCast(const char *operatorName, Oid operatorNamespace,
                           Oid leftObjectId, Oid rightObjectId) {
  static const std::map<Oid, Oid> cast_oids = {{VARCHAROID, TEXTOID}};

  Assert(leftObjectId == rightObjectId);

  auto it = cast_oids.find(leftObjectId);
  if (it != cast_oids.end()) {
    leftObjectId = it->second;
    rightObjectId = it->second;
  }

  Oid opno;
  FmgrInfo finfo;
  CBDB_CHECK(cbdb::PGGetOperator(operatorName, operatorNamespace, leftObjectId,
                                 rightObjectId, &opno, &finfo),
             cbdb::CException::kExTypeInvalid, "Failed to get sort operator");
  return opno;
}

static std::unique_ptr<clustering::DataClustering::DataClusteringOptions>
CreateDataClusteringOptions(const clustering::DataClustering::ClusterType type,
                            Relation rel) {
  auto columns = cbdb::GetClusterColumnIndexes(rel);
  CBDB_CHECK(!columns.empty(), cbdb::CException::kExTypeInvalid,
             "No columns to cluster");
  switch (type) {
    case clustering::DataClustering::ClusterType::kClusterTypeZOrder: {
      auto options = std::make_unique<
          clustering::ZOrderClustering::ZOrderClusteringOptions>(
          rel->rd_att, columns.size(), false, maintenance_work_mem);
      for (int i = 0; i < options->nkeys; i++) {
        // AttrNumer is columns_index + 1
        options->attr[i] = columns[i] + 1;
      }
      return options;
    }
    case clustering::DataClustering::ClusterType::kClusterTypeLexical: {
      auto options = std::make_unique<
          clustering::LexicalClustering::LexicalClusteringOptions>(
          rel->rd_att, columns.size(), false, maintenance_work_mem);

      for (int i = 0; i < options->nkeys; i++) {
        // AttrNumer is columns_index + 1
        options->attr[i] = columns[i] + 1;
        options->sortCollations[i] =
            rel->rd_att->attrs[columns[i]].attcollation;
        Oid type_id = rel->rd_att->attrs[columns[i]].atttypid;

        Oid op_oid = OperatorGetCast(pg_less_operator, PG_CATALOG_NAMESPACE,
                                     type_id, type_id);
        CBDB_CHECK(
            OidIsValid(op_oid), cbdb::CException::kExTypeInvalid,
            pax::fmt("Failed to get sort operator for type %u", type_id));
        options->sortOperators[i] = op_oid;
      }
      return options;
    }
    default:
      CBDB_RAISE(cbdb::CException::kExTypeInvalid, "Unsupported cluster type");
  }
}

clustering::DataClustering::ClusterType GetClusterType(Relation rel) {
  paxc::PaxOptions *pax_options = (paxc::PaxOptions *)rel->rd_options;

  if (strcasecmp(pax_options->cluster_type, PAX_LEXICAL_CLUSTER_TYPE) == 0) {
    return clustering::DataClustering::ClusterType::kClusterTypeLexical;
  } else if (strcasecmp(pax_options->cluster_type, PAX_ZORDER_CLUSTER_TYPE) ==
             0) {
    return clustering::DataClustering::ClusterType::kClusterTypeZOrder;
  }
  CBDB_RAISE(cbdb::CException::kExTypeInvalid, "Unsupported cluster type");
}

void Cluster(Relation rel, Snapshot snapshot, bool is_incremental_cluster) {
  auto columns = cbdb::GetClusterColumnIndexes(rel);
  CBDB_CHECK(!columns.empty(), cbdb::CException::kExTypeInvalid);

  clustering::DataClustering::ClusterType cluster_type = GetClusterType(rel);

  std::vector<MicroPartitionMetadata> delete_files;
  auto iter = MicroPartitionIterator::New(rel, snapshot);
  auto wrap = std::make_unique<FilterIterator<MicroPartitionMetadata>>(
      std::move(iter),
      [&delete_files, is_incremental_cluster](const MicroPartitionMetadata &x) {
        // if is incremental cluster, only cluster the non-clustered blocks
        if (is_incremental_cluster && x.IsClustered()) {
          return false;
        }
        delete_files.push_back(x);
        return true;
      });

  auto reader =
      std::make_unique<clustering::PaxClusteringReader>(rel, std::move(wrap));

  auto writer = std::make_unique<clustering::PaxClusteringWriter>(rel);

  auto options = CreateDataClusteringOptions(cluster_type, rel);

  auto cluster = clustering::DataClustering::CreateDataClustering(cluster_type);

  cluster->Clustering(reader.get(), writer.get(), options.get());

  writer->Close();
  reader->Close();

  std::shared_ptr<IteratorBase<MicroPartitionMetadata>> iter_ptr =
      std::make_shared<VectorIterator<MicroPartitionMetadata>>(
          std::move(delete_files));
  DeleteClusteringFiles(rel, snapshot, iter_ptr);
}

void IndexCluster(Relation old_rel, Relation new_rel, Relation index,
                  Snapshot snapshot) {
  auto iter = MicroPartitionIterator::New(old_rel, snapshot);

  auto reader = std::make_unique<clustering::PaxClusteringReader>(
      old_rel, std::move(iter));

  auto writer = std::make_unique<clustering::PaxClusteringWriter>(new_rel);

  auto cluster = clustering::DataClustering::CreateDataClustering(
      clustering::DataClustering::kClusterTypeIndex);

  clustering::IndexClustering::IndexClusteringOptions options;

  options.tup_desc = old_rel->rd_att;
  options.index_rel = index;
  options.work_mem = maintenance_work_mem;
  cluster->Clustering(reader.get(), writer.get(), &options);

  writer->Close();
  reader->Close();
}

}  // namespace pax
