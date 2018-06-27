#include <CoreFoundation/CoreFoundation.h>
#include <IOKit/IOKitLib.h>
#include <IOKit/storage/IOBlockStorageDriver.h>
#include <IOKit/storage/IOMedia.h>
#include <IOKit/IOBSD.h>

// The iterator of all things disk. Allocated by StartIOCounterFetch, released
// by EndIOCounterFetch.
static io_iterator_t diskIter;

// Begins fetching IO counters.
//
// Returns 1 if the fetch started successfully, false otherwise.
//
// If the fetch was started successfully, you must call EndIOCounterFetch once
// done to release resources.
int StartIOCounterFetch()
{
    if (IOServiceGetMatchingServices(kIOMasterPortDefault,
                                     IOServiceMatching(kIOMediaClass),
                                     &diskIter) != kIOReturnSuccess) {
        return 0;
    }

    return 1;
}

// Releases resources from fetching IO counters.
void EndIOCounterFetch()
{
    IOObjectRelease(diskIter);
}

// The current disk entry of interest. Allocated by FetchNextDisk(), released by
// ReadDiskInfo().
static io_registry_entry_t diskEntry;

// The parent of diskEntry. Same lifetimes.
static io_registry_entry_t parentEntry;

// Fetches the next disk. Note that a disk entry is allocated, and will be held
// until it is processed and freed by ReadDiskInfo.
int FetchNextDisk()
{
    while ((diskEntry = IOIteratorNext(diskIter)) != 0) {
        // We are iterating IOMedia. We need to get the parent too (IOBSD).
        if (IORegistryEntryGetParentEntry(diskEntry, kIOServicePlane, &parentEntry) != kIOReturnSuccess) {
            // something is wrong...
            IOObjectRelease(diskEntry);
            continue;
        }

        if (!IOObjectConformsTo(parentEntry, "IOBlockStorageDriver")) {
            // no use to us, try the next disk
            IOObjectRelease(diskEntry);
            IOObjectRelease(parentEntry);
            continue;
        }

        // Got a disk OK.
        return 1;
    }

    // No more disks.
    return 0;
}

// Reads the current disk (from iteration) info into DiskInfo struct.
// Once done, all resources from the current iteration of reading are freed,
// ready for FetchNextDisk() to be called again.
int ReadDiskInfo(DiskInfo *info)
{
    // Parent props. Allocated by us.
    CFDictionaryRef parentProps = NULL;

    // Disk props. Allocated by us.
    CFDictionaryRef diskProps = NULL;

    // Disk stats, fetched by us, but not allocated by us.
    CFDictionaryRef stats = NULL;

    if (IORegistryEntryCreateCFProperties(diskEntry, (CFMutableDictionaryRef *)&parentProps,
                                          kCFAllocatorDefault, kNilOptions) != kIOReturnSuccess)
    {
        // can't get parent props, give up
        CFRelease(parentProps);
        IOObjectRelease(diskEntry);
        IOObjectRelease(parentEntry);
        return -1;
    }

    if (IORegistryEntryCreateCFProperties(parentEntry, (CFMutableDictionaryRef *)&diskProps,
                                          kCFAllocatorDefault, kNilOptions) != kIOReturnSuccess)
    {
        // can't get disk props, give up
        CFRelease(parentProps);
        CFRelease(diskProps);
        IOObjectRelease(diskEntry);
        IOObjectRelease(parentEntry);
        return -1;
    }

    // Start fetching
    CFStringRef cfDiskName = (CFStringRef)CFDictionaryGetValue(parentProps, CFSTR(kIOBSDNameKey));
    CFStringGetCString(cfDiskName, info->DiskName, MAX_DISK_NAME, CFStringGetSystemEncoding());
    stats = (CFDictionaryRef)CFDictionaryGetValue( diskProps, CFSTR(kIOBlockStorageDriverStatisticsKey));

    if (stats == NULL) {
        // stat fetch failed...
        CFRelease(parentProps);
        CFRelease(diskProps);
        IOObjectRelease(parentEntry);
        IOObjectRelease(diskEntry);
        return -1;
    }

    CFNumberRef cfnum;

    if ((cfnum = (CFNumberRef)CFDictionaryGetValue(stats, CFSTR(kIOBlockStorageDriverStatisticsReadsKey)))) {
        CFNumberGetValue(cfnum, kCFNumberSInt64Type, &info->Reads);
    } else {
        info->Reads = 0;
    }

    if ((cfnum = (CFNumberRef)CFDictionaryGetValue(stats, CFSTR(kIOBlockStorageDriverStatisticsWritesKey)))) {
        CFNumberGetValue(cfnum, kCFNumberSInt64Type, &info->Writes);
    } else {
        info->Writes = 0;
    }

    if ((cfnum = (CFNumberRef)CFDictionaryGetValue(stats, CFSTR(kIOBlockStorageDriverStatisticsBytesReadKey)))) {
        CFNumberGetValue(cfnum, kCFNumberSInt64Type, &info->ReadBytes);
    } else {
        info->ReadBytes = 0;
    }

    if ((cfnum = (CFNumberRef)CFDictionaryGetValue(stats, CFSTR(kIOBlockStorageDriverStatisticsBytesWrittenKey)))) {
        CFNumberGetValue(cfnum, kCFNumberSInt64Type, &info->WriteBytes);
    } else {
        info->WriteBytes = 0;
    }

    if ((cfnum = (CFNumberRef)CFDictionaryGetValue(stats, CFSTR(kIOBlockStorageDriverStatisticsTotalReadTimeKey)))) {
        CFNumberGetValue(cfnum, kCFNumberSInt64Type, &info->ReadTime);
    } else {
        info->ReadTime = 0;
    }
    if ((cfnum = (CFNumberRef)CFDictionaryGetValue(stats, CFSTR(kIOBlockStorageDriverStatisticsTotalWriteTimeKey)))) {
        CFNumberGetValue(cfnum, kCFNumberSInt64Type, &info->WriteTime);
    } else {
        info->WriteTime = 0;
    }

    // note: read/write time are in ns, but we want ms.
    info->ReadTime = info->ReadTime / 1000 / 1000;
    info->WriteTime = info->WriteTime / 1000 / 1000;

    CFRelease(parentProps);
    CFRelease(diskProps);
    IOObjectRelease(parentEntry);
    IOObjectRelease(diskEntry);
    return 0;
}

