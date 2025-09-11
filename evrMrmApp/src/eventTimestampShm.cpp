/*************************************************************************\
* Copyright (c) 2025 European Spallation Source ERIC (ESS), Lund, Sweden
* mrfioc2 is distributed subject to a Software License Agreement found
* in file LICENSE that is included with this distribution.
\*************************************************************************/
/*
 * Serve up EVR event timestamp to the shared memory.
 * Based on ntpShm.cpp.
 *
 * Author: Hinko Kocevar <hinko.kocevar@ess.eu>
 *
 * To use, add to init script:
 *
 *   eventTimestampShm("evrname", sgm_key, sem_key, event)
 *
 * Order of execution in this file.
 * 1) User calls eventTimestampShm() before iocInit()
 * 2) inithooks() is called during iocInit()
 * 3) setup() is called periodically until it succeeds
 * 4) update() is called when event arrives.
 */
#include <stdio.h>
#include <errno.h>

#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <sys/time.h>
#include <time.h>

#include <epicsTime.h>
#include <epicsVersion.h>
#include <epicsMutex.h>
#include <epicsThread.h>
#include <epicsStdio.h> /* redirects stdout/err */
#include <callback.h>
#include <drvSup.h>
#include <recGbl.h>
#include <dbScan.h>
#include <iocsh.h>
#include <initHooks.h>
#include <epicsExport.h>

#include "evr/evr.h"

// Size of shared memory
#define SHM_SIZE 4096

// definition of shared segment
typedef struct {
    struct timespec last_update;

    epicsUInt32 ts_sec;
    epicsUInt32 ts_nsec;
} shmSegment;

typedef struct {
    epicsMutexId mutex;
    CALLBACK cb;
    epicsUInt32 event;
    EVR *evr;
    shmSegment *seg;
    int shm_key;
    int shmid;
    int sem_key;
    int semid;
} shmPriv;

union semun {
    int val;
    struct semid_ds *buf;
    unsigned short *array;
};

static shmPriv shm_;
static epicsThreadOnceId initonce = EPICS_THREAD_ONCE_INIT;

static void update(void*, epicsUInt32 event) {
    if (event != shm_.event) {
        // not our event
        return;
    }

    epicsTimeStamp evrts;
    // read event clock time
    if (! shm_.evr->getTimeStamp(&evrts, shm_.event)) {
        // no valid device time
        return;
    }

    shmSegment *seg = shm_.seg;

    // make timestamp POSIX
    seg->ts_sec = evrts.secPastEpoch + POSIX_TIME_AT_EPICS_EPOCH;
    seg->ts_nsec = evrts.nsec;
    clock_gettime(CLOCK_REALTIME, &seg->last_update);

    // fprintf(stderr, "New timestamp available...\n");
    // Signal consumers
    union semun sem_union;
    sem_union.val = 1;
    if (semctl(shm_.semid, 0, SETVAL, sem_union) == -1) {
        perror("rxBufShm update: semctl SETVAL");
    }
}

static void setup(CALLBACK *) {
    // TODO: We could be more clever and allow reader to create the
    //       segment and semaphore if it starts earlier than us.

    // Create shared memory
    shm_.shmid = shmget(shm_.shm_key, SHM_SIZE, IPC_CREAT | 0666);
    if (shm_.shmid == -1) {
        perror("tsShm setup: shmget");
        return;
    }

    shm_.seg = (shmSegment*)shmat(shm_.shmid, 0, 0);
    if (shm_.seg == (shmSegment*)-1) {
        perror("tsShm setup: shmat");
        return;
    }

    // Create semaphore
    shm_.semid = semget(shm_.sem_key, 1, IPC_CREAT | 0666);
    if (shm_.semid == -1) {
        perror("tsShm setup: semget");
        return;
    }

    // Initialize semaphore
    union semun sem_union;
    sem_union.val = 0;
    if (semctl(shm_.semid, 0, SETVAL, sem_union) == -1) {
        perror("tsShm setup: semctl SETVAL");
        return;
    }

    try {
        shm_.evr->eventNotifyAdd(shm_.event, &update, 0);
    } catch(std::exception& e) {
        fprintf(stderr, "Error registering for event %d: %s\n", shm_.event, e.what());
    }
}

static void shminit(void*) {
    shm_.mutex = epicsMutexMustCreate();

    callbackSetPriority(priorityLow, &shm_.cb);
    callbackSetCallback(&setup, &shm_.cb);
    callbackSetUser(0, &shm_.cb);
}

static void inithooks(initHookState state) {
    if (state != initHookAfterIocRunning)
        return;

    epicsThreadOnce(&initonce, &shminit, 0);

    epicsMutexMustLock(shm_.mutex);
    if (shm_.evr) {
        callbackRequest(&shm_.cb);
        fprintf(stderr, "Starting timestamp SHM writer...\n");
    }
    epicsMutexUnlock(shm_.mutex);
}

static void create(const char* evrname, const int shm_key, const int sem_key, const int event) {
    try {
        if (event <= 0 || event > 255) {
            fprintf(stderr, "Invalid event # %d\n", event);
            return;
        }
        mrf::Object *obj = mrf::Object::getObject(evrname);
        if (! obj) {
            fprintf(stderr, "Unknown EVR: %s\n", evrname);
            return;
        }

        EVR *evr = dynamic_cast<EVR*>(obj);
        if (! evr) {
            fprintf(stderr, "\"%s\" is not an EVR\n", evrname);
            return;
        }

        epicsThreadOnce(&initonce, &shminit, 0);

        epicsMutexMustLock(shm_.mutex);

        if (shm_.evr) {
            epicsMutexUnlock(shm_.mutex);
            fprintf(stderr, "shm_ already initialized.\n");
            return;
        }

        shm_.shm_key = shm_key;
        shm_.sem_key = sem_key;
        shm_.event = event;
        shm_.evr = evr;

        epicsMutexUnlock(shm_.mutex);
    } catch(std::exception& e) {
        fprintf(stderr, "Error: %s\n", e.what());
    }
}

static const iocshArg arg0 = {"evr name", iocshArgString};
static const iocshArg arg1 = {"shm key", iocshArgInt};
static const iocshArg arg2 = {"semaphore key", iocshArgInt};
static const iocshArg arg3 = {"event code", iocshArgInt};
static const iocshArg * const args[4] = {&arg0, &arg1, &arg2, &arg3};
static const iocshFuncDef fdef = {"eventTimestampShm", 4, args};

static void fcall(const iocshArgBuf *args) {
    create(args[0].sval, args[1].ival, args[2].ival, args[3].ival);
}

static void eventTimestampShmRegister() {
    initHookRegister(&inithooks);
    iocshRegister(&fdef, &fcall);
}

extern "C"{
    epicsExportRegistrar(eventTimestampShmRegister);
}
