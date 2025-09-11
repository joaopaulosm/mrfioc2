/*************************************************************************\
* Copyright (c) 2025 European Spallation Source ERIC (ESS), Lund, Sweden
* mrfioc2 is distributed subject to a Software License Agreement found
* in file LICENSE that is included with this distribution.
\*************************************************************************/
/*
 * Serve up EVR RX buffer to the shared memory.
 * Based on ntpShm.cpp.
 *
 * Author: Hinko Kocevar <hinko.kocevar@ess.eu>
 *
 * To use, add to init script:
 *
 *   rxBufferShm("evrname", sgm_key, sem_key)
 *
 * Order of execution in this file.
 * 1) User calls rxBufferShm() before iocInit()
 * 2) inithooks() is called during iocInit()
 * 3) setup() is called periodically until it succeeds
 * 4) update() is called when RX buffer arrives.
 */
#include <stdio.h>
#include <errno.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <sys/time.h>
#include <time.h>
#include <arpa/inet.h>

#include <epicsTime.h>
#include <epicsVersion.h>
#include <epicsMutex.h>
#include <epicsThread.h>
#include <epicsStdio.h> /* redirects stdout/err */
#include <callback.h>
#include <iocsh.h>
#include <initHooks.h>
#include <epicsExport.h>
#include <devMrmBuf.h>

// Size of shared memory
#define SHM_SIZE 4096

// definition of shared segment
typedef struct {
    struct timespec mTimespec;
    // EVR RX buffer is 2048 bytes max
    epicsUInt32 words[512];
} shmSegment;

typedef struct {
    epicsMutexId mutex;
    CALLBACK cb;
    mrmBufferInfo_t *buffer_info;
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
static epicsThreadOnceId daqonce = EPICS_THREAD_ONCE_INIT;

static void update(void *arg, epicsStatus status, epicsUInt32 length, const epicsUInt8 *buffer) {
    // volatile operations aren't really enough, but will have to do.
    volatile shmSegment *seg = shm_.seg;

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    seg->mTimespec.tv_sec = ts.tv_sec;
    seg->mTimespec.tv_nsec = ts.tv_nsec;

    if (length % 4) {
        fprintf(stderr, "rxBufShm: RX buffer length (%d) should be divisible by 4!\n", length);
    }
    // RX buffer is received as an array of 32 bit network (big) endian values so
    // convert them to host endian.
    epicsUInt32 *src = (epicsUInt32 *)buffer;
    epicsUInt32 *dst = (epicsUInt32 *)seg->words;
    for (epicsUInt32 i = 0; i < length >> 2; i++) {
        *dst = ntohl(*src);
        dst++;
        src++;
    }

    // fprintf(stderr, "New RX buffer available (len %d)...\n", length);
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
        perror("rxBufShm setup: shmget");
        return;
    }

    shm_.seg = (shmSegment*)shmat(shm_.shmid, 0, 0);
    if (shm_.seg == (shmSegment*)-1) {
        perror("rxBufShm setup: shmat");
        return;
    }

    // Create semaphore
    shm_.semid = semget(shm_.sem_key, 1, IPC_CREAT | 0666);
    if (shm_.semid == -1) {
        perror("rxBufShm setup: semget");
        return;
    }

    // Initialize semaphore
    union semun sem_union;
    sem_union.val = 0;
    if (semctl(shm_.semid, 0, SETVAL, sem_union) == -1) {
        perror("rxBufShm setup: semctl SETVAL");
        return;
    }

    if (mrmBufRegCallback(shm_.buffer_info, &update, 0) == -1) {
        // TODO: remove the SHM region and semaphore here?
        free(shm_.buffer_info);
        shm_.buffer_info = NULL;
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

    epicsThreadOnce(&daqonce, &shminit, 0);

    epicsMutexMustLock(shm_.mutex);
    if (shm_.buffer_info) {
        callbackRequest(&shm_.cb);
        fprintf(stderr, "Starting RX buffer SHM writer...\n");
    }
    epicsMutexUnlock(shm_.mutex);
}

static void create(const char* evrname, const int shm_key, const int sem_key) {
    
    epicsThreadOnce(&daqonce, &shminit, 0);
    
    epicsMutexMustLock(shm_.mutex);

    shm_.shm_key = shm_key;
    shm_.sem_key = sem_key;

    if (shm_.buffer_info) {
        epicsMutexUnlock(shm_.mutex);
        fprintf(stderr, "rxBufShm create: buffer info already initialized\n");
        return;
    }

    shm_.buffer_info = mrmBufInit(evrname);
    if (! shm_.buffer_info) {
        fprintf(stderr, "rxBufShm create: failed to create buffer info\n");
    }
    epicsMutexUnlock(shm_.mutex);
}

static const iocshArg arg0 = {"evr name", iocshArgString};
static const iocshArg arg1 = {"shm key", iocshArgInt};
static const iocshArg arg2 = {"semaphore key", iocshArgInt};
static const iocshArg * const args[3] = {&arg0, &arg1, &arg2};
static const iocshFuncDef fdef = {"rxBufferShm", 3, args};

static void fcall(const iocshArgBuf *args) {
    create(args[0].sval, args[1].ival, args[2].ival);
}

static void rxBufferShmRegister() {
    initHookRegister(&inithooks);
    iocshRegister(&fdef, &fcall);
}

extern "C"{
    epicsExportRegistrar(rxBufferShmRegister);
}
