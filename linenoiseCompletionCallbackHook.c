
#include "linenoiseCompletionCallbackHook.h"

extern void linenoiseGoCompletionCallbackHook(const char *, linenoiseCompletions *);

void linenoiseSetupCompletionCallbackHook() {
	linenoiseSetCompletionCallback(linenoiseGoCompletionCallbackHook);
}