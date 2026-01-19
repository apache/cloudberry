#include "google/protobuf/reflection.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/timestamp.pb.h"

#include "LogSchema.h"

const std::unordered_map<std::string_view, size_t> &proto_name_to_col_idx() {
  static const auto name_col_idx = [] {
    std::unordered_map<std::string_view, size_t> map;
    map.reserve(log_tbl_desc.size());

    for (size_t idx = 0; idx < natts_yagp_log; ++idx) {
      map.emplace(log_tbl_desc[idx].proto_field_name, idx);
    }

    return map;
  }();
  return name_col_idx;
}

TupleDesc DescribeTuple() {
  TupleDesc tupdesc = CreateTemplateTupleDesc(natts_yagp_log);

  for (size_t anum = 1; anum <= natts_yagp_log; ++anum) {
    TupleDescInitEntry(tupdesc, anum, log_tbl_desc[anum - 1].pg_att_name.data(),
                       log_tbl_desc[anum - 1].type_oid, -1 /* typmod */,
                       0 /* attdim */);
  }

  return tupdesc;
}

Datum protots_to_timestamptz(const google::protobuf::Timestamp &ts) {
  TimestampTz pgtimestamp =
      (TimestampTz)ts.seconds() * USECS_PER_SEC + (ts.nanos() / 1000);
  pgtimestamp -= (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY;
  return TimestampTzGetDatum(pgtimestamp);
}

Datum field_to_datum(const google::protobuf::FieldDescriptor *field,
                     const google::protobuf::Reflection *reflection,
                     const google::protobuf::Message &msg) {
  using namespace google::protobuf;

  switch (field->cpp_type()) {
  case FieldDescriptor::CPPTYPE_INT32:
    return Int32GetDatum(reflection->GetInt32(msg, field));
  case FieldDescriptor::CPPTYPE_INT64:
    return Int64GetDatum(reflection->GetInt64(msg, field));
  case FieldDescriptor::CPPTYPE_UINT32:
    return Int64GetDatum(reflection->GetUInt32(msg, field));
  case FieldDescriptor::CPPTYPE_UINT64:
    return Int64GetDatum(
        static_cast<int64_t>(reflection->GetUInt64(msg, field)));
  case FieldDescriptor::CPPTYPE_DOUBLE:
    return Float8GetDatum(reflection->GetDouble(msg, field));
  case FieldDescriptor::CPPTYPE_FLOAT:
    return Float4GetDatum(reflection->GetFloat(msg, field));
  case FieldDescriptor::CPPTYPE_BOOL:
    return BoolGetDatum(reflection->GetBool(msg, field));
  case FieldDescriptor::CPPTYPE_ENUM:
    return CStringGetTextDatum(reflection->GetEnum(msg, field)->name().data());
  case FieldDescriptor::CPPTYPE_STRING:
    return CStringGetTextDatum(reflection->GetString(msg, field).c_str());
  default:
    return (Datum)0;
  }
}

void process_field(const google::protobuf::FieldDescriptor *field,
                   const google::protobuf::Reflection *reflection,
                   const google::protobuf::Message &msg,
                   const std::string &field_name, Datum *values, bool *nulls) {

  auto proto_idx_map = proto_name_to_col_idx();
  auto it = proto_idx_map.find(field_name);

  if (it == proto_idx_map.end()) {
    ereport(NOTICE,
            (errmsg("YAGPCC protobuf field %s is not registered in log table",
                    field_name.c_str())));
    return;
  }

  int idx = it->second;

  if (!reflection->HasField(msg, field)) {
    nulls[idx] = true;
    return;
  }

  if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE &&
      field->message_type()->full_name() == "google.protobuf.Timestamp") {
    const auto &ts = static_cast<const google::protobuf::Timestamp &>(
        reflection->GetMessage(msg, field));
    values[idx] = protots_to_timestamptz(ts);
  } else {
    values[idx] = field_to_datum(field, reflection, msg);
  }
  nulls[idx] = false;

  return;
}

void extract_query_req(const google::protobuf::Message &msg,
                       const std::string &prefix, Datum *values, bool *nulls) {
  using namespace google::protobuf;

  const Descriptor *descriptor = msg.GetDescriptor();
  const Reflection *reflection = msg.GetReflection();

  for (int i = 0; i < descriptor->field_count(); ++i) {
    const FieldDescriptor *field = descriptor->field(i);

    // For now, we do not log any repeated fields plus they need special
    // treatment.
    if (field->is_repeated()) {
      continue;
    }

    std::string curr_pref = prefix.empty() ? "" : prefix + ".";
    std::string field_name = curr_pref + field->name().data();

    if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE &&
        field->message_type()->full_name() != "google.protobuf.Timestamp") {

      if (reflection->HasField(msg, field)) {
        const Message &nested = reflection->GetMessage(msg, field);
        extract_query_req(nested, field_name, values, nulls);
      }
    } else {
      process_field(field, reflection, msg, field_name, values, nulls);
    }
  }
}
