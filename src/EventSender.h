#pragma once

#include <memory>

class GrpcConnector;

struct QueryDesc;

class EventSender
{
public:
    void ExecutorStart(QueryDesc *query_desc, int eflags);
    void ExecutorFinish(QueryDesc *query_desc);
    static EventSender *instance();

private:
    EventSender();
    std::unique_ptr<GrpcConnector> connector;
};