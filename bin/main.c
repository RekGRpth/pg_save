#include "bin.h"

int main(int argc, char *argv[]) {
    pg_logging_init(argv[0]);
    W("hi");
    return 0;
}
