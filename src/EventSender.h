#pragma once

struct QueryDesc;

class EventSender {
public:
    void ExecutorStart(QueryDesc *queryDesc, int eflags);
    void ExecutorFinish(QueryDesc *queryDesc);

    static EventSender *instance() {
        static EventSender sender;
        return &sender;
    }
private:
    EventSender() {}
};