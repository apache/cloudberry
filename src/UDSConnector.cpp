#include "UDSConnector.h"
#include "Config.h"

#include <string>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/fcntl.h>
#include <chrono>
#include <thread>

extern "C" {
#include "postgres.h"
#include "cdb/cdbvars.h"
}

UDSConnector::UDSConnector() : uds_path("unix://" + Config::uds_path()) {
  GOOGLE_PROTOBUF_VERIFY_VERSION;
}

void UDSConnector::report_query(std::queue<yagpcc::SetQueryReq> &reqs,
                                const std::string &event) {
  while (!reqs.empty()) {
    const auto &req = reqs.front();
    if (report_query(req, event)) {
      reqs.pop();
    } else {
      break;
    }
  }
}

static void inline log_tracing_failure(const yagpcc::SetQueryReq &req,
                                       const std::string &event) {
  ereport(LOG,
          (errmsg("Query {%d-%d-%d} %s tracing failed with error %s",
                  req.query_key().tmid(), req.query_key().ssid(),
                  req.query_key().ccnt(), event.c_str(), strerror(errno))));
}

bool UDSConnector::report_query(const yagpcc::SetQueryReq &req,
                                const std::string &event) {
  sockaddr_un address;
  address.sun_family = AF_UNIX;
  strcpy(address.sun_path, uds_path.c_str());
  bool success = true;
  auto sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (sockfd != -1) {
    if (fcntl(sockfd, F_SETFL, O_NONBLOCK) != -1) {
      if (connect(sockfd, (sockaddr *)&address, sizeof(address)) != -1) {
        auto data_size = req.ByteSize();
        auto total_size = data_size + sizeof(uint32_t);
        uint8_t *buf = (uint8_t *)palloc(total_size);
        uint32_t *size_payload = (uint32_t *)buf;
        *size_payload = data_size;
        req.SerializeWithCachedSizesToArray(buf + sizeof(uint32_t));
        int64_t sent = 0, sent_total = 0;
        do {
          sent = send(sockfd, buf + sent_total, total_size - sent_total,
                      MSG_DONTWAIT);
          sent_total += sent;
        } while (
            sent > 0 && size_t(sent_total) != total_size &&
            // the line below is a small throttling hack:
            // if a message does not fit a single packet, we take a nap
            // before sending the next one.
            // Otherwise, MSG_DONTWAIT send might overflow the UDS
            (std::this_thread::sleep_for(std::chrono::milliseconds(1)), true));
        if (sent < 0) {
          log_tracing_failure(req, event);
          success = false;
        }
        pfree(buf);
      } else {
        // log the error and go on
        log_tracing_failure(req, event);
        success = false;
      }
    } else {
      // That's a very important error that should never happen, so make it
      // visible to an end-user and admins.
      ereport(WARNING,
              (errmsg("Unable to create non-blocking socket connection %s",
                      strerror(errno))));
      success = false;
    }
    close(sockfd);
  } else {
    // log the error and go on
    log_tracing_failure(req, event);
    success = false;
  }
  return success;
}