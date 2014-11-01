package linenoise

// -windows

// #include <stdlib.h>
// #include "linenoise.h"
// #include "linenoiseCompletionCallbackHook.h"
import "C"

import (
	"errors"
	"unsafe"
)

// KillSignalError is returned returned by Line() when a user quits from prompt.
// This occurs when the user enters ctrl+C or ctrl+D.
var KillSignalError = errors.New("prompt was quited with a killsignal")

func init() {
	C.linenoiseSetupCompletionCallbackHook()
}

// Line displays given string and returns line from user input.
func Line(prompt string) (string, error) { // char *linenoise(const char *prompt);
	promptCString := C.CString(prompt)
	resultCString := C.linenoise(promptCString)
	C.free(unsafe.Pointer(promptCString))
	defer C.free(unsafe.Pointer(resultCString))

	if resultCString == nil {
		return "", KillSignalError
	}

	result := C.GoString(resultCString)

	return result, nil
}

// AddHistory adds a line to history. Returns non-nil error on fail.
func AddHistory(line string) error { // int linenoiseHistoryAdd(const char *line);
	lineCString := C.CString(line)
	res := C.linenoiseHistoryAdd(lineCString)
	C.free(unsafe.Pointer(lineCString))
	if res != 1 {
		return errors.New("Could not add line to history.")
	}
	return nil
}

// SetHistoryCapacity changes the maximum length of history. Returns non-nil error on fail.
func SetHistoryCapacity(capacity int) error { // int linenoiseHistorySetMaxLen(int len);
	res := C.linenoiseHistorySetMaxLen(C.int(capacity))
	if res != 1 {
		return errors.New("Could not set history max len.")
	}
	return nil
}

// SaveHistory saves from file with given filename. Returns non-nil error on fail.
func SaveHistory(filename string) error { // int linenoiseHistorySave(char *filename);
	filenameCString := C.CString(filename)
	res := C.linenoiseHistorySave(filenameCString)
	C.free(unsafe.Pointer(filenameCString))
	if res != 0 {
		return errors.New("Could not save history to file.")
	}
	return nil
}

// LoadHistory loads from file with given filename. Returns non-nil error on fail.
func LoadHistory(filename string) error { // int linenoiseHistoryLoad(char *filename);
	filenameCString := C.CString(filename)
	res := C.linenoiseHistoryLoad(filenameCString)
	C.free(unsafe.Pointer(filenameCString))
	if res != 0 {
		return errors.New("Could not load history from file.")
	}
	return nil
}

// Clear clears the screen.
func Clear() { // void linenoiseClearScreen(void);
	C.linenoiseClearScreen()
}

// SetMultiline sets linenoise to multiline or single line.
// In multiline mode the user input will be wrapped to a new line when the length exceeds the amount of available rows in the terminal.
func SetMultiline(ml bool) { // void linenoiseSetMultiLine(int ml);
	if ml {
		C.linenoiseSetMultiLine(1)
	} else {
		C.linenoiseSetMultiLine(0)
	}
}

// CompletionHandler provides possible completions for given input
type CompletionHandler func(input string) []string

// DefaultCompletionHandler simply returns an empty slice.
var DefaultCompletionHandler = func(input string) []string {
	return make([]string, 0)
}

var complHandler = DefaultCompletionHandler

// SetCompletionHandler sets the CompletionHandler to be used for completion
func SetCompletionHandler(c CompletionHandler) {
	complHandler = c
}

// typedef struct linenoiseCompletions {
//   size_t len;
//   char **cvec;
// } linenoiseCompletions;
// typedef void(linenoiseCompletionCallback)(const char *, linenoiseCompletions *);
// void linenoiseSetCompletionCallback(linenoiseCompletionCallback *);
// void linenoiseAddCompletion(linenoiseCompletions *, char *);

//export linenoiseGoCompletionCallbackHook
func linenoiseGoCompletionCallbackHook(input *C.char, completions *C.linenoiseCompletions) {
	completionsSlice := complHandler(C.GoString(input))

	completionsLen := len(completionsSlice)
	completions.len = C.size_t(completionsLen)

	if completionsLen > 0 {
		cvec := C.malloc(C.size_t(int(unsafe.Sizeof(*(**C.char)(nil))) * completionsLen))
		cvecSlice := (*(*[999999]*C.char)(cvec))[:completionsLen]

		for i, str := range completionsSlice {
			cvecSlice[i] = C.CString(str)
		}
		completions.cvec = (**C.char)(cvec)
	}
}

// PrintKeyCodes puts linenoise in key codes debugging mode.
// Press keys and key combinations to see key codes. Type 'quit' at any time to exit.
// PrintKeyCodes blocks until user enters 'quit'.
func PrintKeyCodes() { // void linenoisePrintKeyCodes(void);
	C.linenoisePrintKeyCodes()
}
