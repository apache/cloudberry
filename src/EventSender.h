#pragma once

#include <memory>

class GrpcConnector;

struct QueryDesc;

class EventSender {
public:
    void ExecutorStart(QueryDesc *queryDesc, int eflags);
    void ExecutorFinish(QueryDesc *queryDesc);
    static EventSender *instance();

private:
    EventSender();
    std::unique_ptr<GrpcConnector> connector;
};