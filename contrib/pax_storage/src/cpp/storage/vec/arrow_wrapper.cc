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
 * arrow_wrapper.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec/arrow_wrapper.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/vec/arrow_wrapper.h"

#ifdef VEC_BUILD

#include "comm/pax_memory.h"
#include "storage/pax_buffer.h"

/// export interface wrapper of arrow
namespace arrow {

void ExportArrayRelease(ArrowArray *array) {
  // The Exception throw from this call back won't be catch
  // Because caller will call this callback in destructor
  // just let long jump happen
  if (array->children) {
    for (int64_t i = 0; i < array->n_children; i++) {
      if (array->children[i] && array->children[i]->release) {
        array->children[i]->release(array->children[i]);
      }
    }

    pax::PAX_DELETE_ARRAY<ArrowArray *>(array->children);
  }

  if (array->buffers) {
    for (int64_t i = 0; i < array->n_buffers; i++) {
      if (array->buffers[i]) {
        char *temp = const_cast<char *>((const char *)array->buffers[i]);
        pax::BlockBuffer::Free(temp);
      }
    }
    char **temp = const_cast<char **>((const char **)array->buffers);
    pax::PAX_DELETE_ARRAY<char *>(temp);
  }

  if (array->dictionary) {
    pax::PAX_DELETE<ArrowArray>(array->dictionary);
  }

  array->release = NULL;
  if (array->private_data) {
    ArrowArray *temp = static_cast<ArrowArray *>(array->private_data);
    pax::PAX_DELETE<ArrowArray>(temp);
  }
};

static void ExportArrayNodeDetails(ArrowArray *export_array,
                                   const std::shared_ptr<ArrayData> &data,
                                   const std::vector<ArrowArray *> &child_array,
                                   bool is_child) {
  export_array->length = data->length;
  export_array->null_count = data->null_count;
  export_array->offset = data->offset;

  export_array->n_buffers = static_cast<int64_t>(data->buffers.size());
  export_array->n_children = static_cast<int64_t>(child_array.size());
  export_array->buffers =
      export_array->n_buffers
          ? (const void **)pax::PAX_NEW_ARRAY<char *>(export_array->n_buffers)
          : nullptr;

  for (int64_t i = 0; i < export_array->n_buffers; i++) {
    auto buffer = data->buffers[i];
    export_array->buffers[i] = buffer ? buffer->data() : nullptr;
  }

  export_array->children =
      export_array->n_children
          ? pax::PAX_NEW_ARRAY<ArrowArray *>(export_array->n_children)
          : nullptr;
  for (int64_t i = 0; i < export_array->n_children; i++) {
    export_array->children[i] = child_array[i];
  }

  if (data->dictionary) {
    ArrowArray *export_array_dict = pax::PAX_NEW<ArrowArray>();
    std::vector<ArrowArray *> child_array_dict;
    ExportArrayNodeDetails(export_array_dict, data->dictionary,
                           child_array_dict, true);

    export_array->dictionary = export_array_dict;
  } else {
    export_array->dictionary = nullptr;
  }

  export_array->private_data = is_child ? (void *)export_array : nullptr;
  export_array->release = ExportArrayRelease;
}

static ArrowArray *ExportArrayNode(const std::shared_ptr<ArrayData> &data) {
  ArrowArray *export_array;
  std::vector<ArrowArray *> child_array;

  for (size_t i = 0; i < data->child_data.size(); ++i) {
    child_array.emplace_back(ExportArrayNode(data->child_data[i]));
  }

  export_array = pax::PAX_NEW<ArrowArray>();
  ExportArrayNodeDetails(export_array, data, child_array, true);
  return export_array;
}

void ExportArrayRoot(const std::shared_ptr<ArrayData> &data,
                     ArrowArray *export_array) {
  std::vector<ArrowArray *> child_array;

  for (size_t i = 0; i < data->child_data.size(); ++i) {
    child_array.emplace_back(ExportArrayNode(data->child_data[i]));
  }
  Assert(export_array);

  ExportArrayNodeDetails(export_array, data, child_array, false);
}

int FindFieldIndex(
    const std::vector<std::pair<const char *, size_t>> &table_names,
    const std::pair<const char *, size_t> &kname) {
  auto num = table_names.size();

  for (size_t i = 0; i < num; i++) {
    const auto &fname = table_names[i];
    if (fname.second == kname.second &&
        memcmp(fname.first, kname.first, fname.second) == 0)
      return i;
  }

  if (kname.second == 4 && memcmp(kname.first, "ctid", 4) == 0)
    return SelfItemPointerAttributeNumber;

  throw std::string("Not found field name:") + kname.first;
}

std::pair<const char *, size_t> ExtractFieldName(const std::string &name) {
  const char *p = name.c_str();
  auto idx = name.find_last_of('(');

  if (idx == std::string::npos) return {p, name.size()};

  return {p, idx};
}

}  // namespace arrow

#endif  // VEC_BUILD
