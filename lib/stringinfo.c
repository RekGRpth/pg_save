#include "lib.h"

void initStringInfoMy(MemoryContext memoryContext, StringInfoData *buf) {
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(memoryContext);
    initStringInfo(buf);
    MemoryContextSwitchTo(oldMemoryContext);
}
