/*************************************************************************\
* Copyright (c) 2013 Brookhaven Science Associates, as Operator of
*     Brookhaven National Laboratory.
* mrfioc2 is distributed subject to a Software License Agreement found
* in file LICENSE that is included with this distribution.
\*************************************************************************/
/*
 * Serve up EVR time to the shared memory driver (#28) of the NTP daemon.
 *
 * cf. http://www.eecis.udel.edu/~mills/ntp/html/drivers/driver28.html
 *
 * Author: Michael Davidsaver <mdavidsaver@gmail.com>
 *
 * To use, add to init script.  Where 0<=N<=4.  To use 0 or 1 the IOC
 * must run as root.
 *
 *   time2ntp("evrname", N)
 *
 * Add to NTP daemon config.  Replace 'prefer' with 'noselect' when testing
 *
 *   server 127.127.28.N minpoll 1 maxpoll 2 prefer
 *   fudge 127.127.28.N refid EVR
 *
 * Order of execution in this file.
 * 1) User calls time2ntp() before iocInit()
 * 2) ntpshmhooks() is called during iocInit()
 * 3) ntpsetup() is called periodically until it succeeds
 * 4) ntpshmupdate() is called once per second.
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
#include <longinRecord.h>
#include <aiRecord.h>
#include <menuConvert.h>
#include <dbScan.h>
#include <iocsh.h>
#include <initHooks.h>

#include "evr/evr.h"

// NTPD itself doesn't make much attempt at synchronizing
// but we try to do better
#if defined(__GNUC__)
#  if ( ( __GNUC__ * 100 + __GNUC_MINOR__ ) >= 401 )
#    define SYNC() __sync_synchronize()
#  else
#    define SYNC() __asm__ __volatile__ ("":::"memory")
#  endif
#else
   // oh well, we tried
#  define SYNC() do{}while(0)
#endif

// from a shell run 'ipcs -m' while NTPD is running w/ driver28 loaded.
// #define NTPD_SEG0 0x4E545030

#define RETRY_TIME 30.0

// definition of shared segment, as described in
// http://www.eecis.udel.edu/~mills/ntp/html/drivers/driver28.html
typedef struct {
    epicsUInt32 mShmTimeStampSec;
    epicsUInt32 mShmTimeStampNsec;
    struct timespec mTimespec;

    int mode;
    int count;

    // The EVR timestamp
    time_t stampSec;
    int stampUsec;

    // The system time when it was acquired
    time_t rxSec;
    int rxUsec;

    int leap;
    int precision;
    int nsamples;
    int valid;

    int pad[10];
} shmSegment;

static epicsThreadOnceId daqonce = EPICS_THREAD_ONCE_INIT;

typedef struct {

    epicsMutexId daqlock;

    CALLBACK daqcb;

    epicsUInt32 event;

    EVR *evr;
    int segid;

    shmSegment* seg;

    int notify_nomap;
    int notify_1strx;

    // IOSCANPVT lastUpdate;

    bool lastValid;
    epicsTimeStamp lastStamp;
    epicsTimeStamp lastRx;

    unsigned int numOk;
    unsigned int numFail;

    int shmid;
    int semid;

} daqShmPriv;
static daqShmPriv daqShm;

#define SHM_KEY 0x1234      // Key for shared memory
#define SEM_KEY 0x5678      // Key for semaphore
#define SHM_SIZE 1024       // Size of shared memory

union semun {
    int val;
    struct semid_ds *buf;
    unsigned short *array;
};

// Function to initialize the semaphore
void init_semaphore(int semid) {
    union semun sem_union;
    sem_union.val = 0;
    if (semctl(semid, 0, SETVAL, sem_union) == -1) {
        perror("semctl SETVAL");
        exit(1);
    }
}

// Function to signal (increment) the semaphore
void signal_semaphore(int semid) {
    // struct sembuf sb = {0, 1, 0}; // Increment semaphore
    // if (semop(semid, &sb, 1) == -1) {
    //     perror("semop signal");
    //     exit(1);
    // }
    union semun sem_union;
    sem_union.val = 1;
    if (semctl(semid, 0, SETVAL, sem_union) == -1) {
        perror("semctl SETVAL");
        exit(1);
    }
}


static void incFail()
{
    epicsMutexMustLock(daqShm.daqlock);
    daqShm.lastValid = false;
    daqShm.numFail++;
    epicsMutexUnlock(daqShm.daqlock);
    printf("numFail %d\n", daqShm.numFail);
}

static void daqshmupdate(void*, epicsUInt32 event)
{
    if(event!=daqShm.event) {
        incFail(); return;
    }

    epicsTimeStamp evrts;
    // if(!daqShm.evr->getTimeStamp(&evrts, 0)) // read current wall clock time
    // {
    //     // no valid device time
    //     incFail(); return;
    // }
    if(!daqShm.evr->getTimeStamp(&evrts, daqShm.event)) // read event clock time
    {
        // no valid device time
        incFail(); return;
    }


    // struct timeval cputs;
    // if(gettimeofday(&cputs, 0))
    // {
    //     // no valid cpu time?
    //     incFail(); return;
    // }

    // struct timeval evrts_posix;
    // evrts_posix.tv_sec = evrts.secPastEpoch + POSIX_TIME_AT_EPICS_EPOCH;
    // evrts_posix.tv_usec = evrts.nsec / 1000;
    // // correct for bias in truncation
    // if(evrts.nsec % 1000 >= 500) {
    //     evrts_posix.tv_usec += 1;
    //     if(evrts_posix.tv_usec>=1000000) {
    //         evrts_posix.tv_sec += 1;
    //         evrts_posix.tv_usec = 0;
    //     }
    // }

    // volatile operations aren't really enough, but will have to do.
    volatile shmSegment* seg=daqShm.seg;

    // seg->valid = 0;
    // SYNC();
    // int c1 = seg->count++;
    // seg->stampSec = evrts_posix.tv_sec;
    // seg->stampUsec = evrts_posix.tv_usec;
    // seg->rxSec = cputs.tv_sec;
    // seg->rxUsec = cputs.tv_usec;
    // int c2 = seg->count++;
    // if(c1+1!=c2) {
    //     fprintf(stderr, "daqshmupdate: possible collision with another writer!\n");
    //     incFail(); return;
    // }
    seg->mShmTimeStampSec = evrts.secPastEpoch;
    seg->mShmTimeStampNsec = evrts.nsec;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    //memcpy(&seg->mTimespec, &ts, sizeof(ts));
    seg->mTimespec.tv_sec = ts.tv_sec;
    seg->mTimespec.tv_nsec = ts.tv_nsec;

    // seg->valid = 1;
    // SYNC();

    // epicsMutexMustLock(daqShm.daqlock);
    // daqShm.lastValid = true;
    daqShm.numOk++;
    // daqShm.lastStamp = evrts;
    // daqShm.lastRx = epicsTime(cputs);
    // epicsMutexUnlock(daqShm.daqlock);

    // scanIoRequest(daqShm.lastUpdate);

    if(!daqShm.notify_1strx) {
        fprintf(stderr, "First timestamp available...\n");
        daqShm.notify_1strx = 1;
    }

    // Signal consumers
    // fprintf(stderr, "New timestamp available...\n");
    signal_semaphore(daqShm.semid);

    return; // normal exit
}

static void daqsetup(CALLBACK *)
{
    // We don't set IPC_CREAT, but instead wait for NTPD to start and initialize
    // as it wants
    // int mode = daqShm.segid <=1 ? 0600 : 0666;

    // int shmid = shmget((key_t)(NTPD_SEG0+daqShm.segid), sizeof(shmSegment), mode);

    // if(shmid==-1) {
    //     if(errno==ENOENT) {
    //         if(!daqShm.notify_nomap) {
    //             fprintf(stderr, "Can't find shared memory segment.  Either NTPD hasn't started,"
    //                     " or is not configured correctly.  Will retry later.");
    //             daqShm.notify_nomap = 1;
    //         }
    //         callbackRequestDelayed(&daqShm.daqcb, RETRY_TIME);
    //     } else {
    //         perror("daqshmsetup: shmget");
    //     }
    //     return;
    // }

    // daqShm.seg = (shmSegment*)shmat(shmid, 0, 0);
    // if(daqShm.seg==(shmSegment*)-1) {
    //     perror("daqshmsetup: shmat");
    //     return;
    // }

    // TODO: We could be more clever and allow reader to create the
    //       segment and semaphore if it starts earlier than us.

    // Create shared memory
    daqShm.shmid = shmget(SHM_KEY, SHM_SIZE, IPC_CREAT | 0666);
    if (daqShm.shmid == -1) {
        perror("daqshmsetup: shmget");
        return;
    }

    daqShm.seg = (shmSegment*)shmat(daqShm.shmid, 0, 0);
    if (daqShm.seg == (shmSegment*)-1) {
        perror("daqshmsetup: shmat");
        return;
    }

    // Create semaphore
    daqShm.semid = semget(SEM_KEY, 1, IPC_CREAT | 0666);
    if (daqShm.semid == -1) {
        perror("daqshmsetup: semget");
        return;
    }

    // Initialize semaphore
    init_semaphore(daqShm.semid);

    daqShm.seg->mode = 1;
    daqShm.seg->valid = 0;
    SYNC();
    daqShm.seg->leap = 0; //TODO: what does this do?
    daqShm.seg->precision = -19; // pow(2,-19) ~= 1e-6 sec
    daqShm.seg->nsamples = 3; //TODO: what does this do?
    SYNC();

    try {
        daqShm.evr->eventNotifyAdd(daqShm.event, &daqshmupdate, 0);
    } catch(std::exception& e) {
        fprintf(stderr, "Error registering for event %d: %s\n", daqShm.event, e.what());
    }
}

static void daqshminit(void*)
{
    daqShm.daqlock = epicsMutexMustCreate();

    callbackSetPriority(priorityLow, &daqShm.daqcb);
    callbackSetCallback(&daqsetup, &daqShm.daqcb);
    callbackSetUser(0, &daqShm.daqcb);
}

static void daqshmhooks(initHookState state)
{
    if(state!=initHookAfterIocRunning)
        return;

    epicsThreadOnce(&daqonce, &daqshminit, 0);

    epicsMutexMustLock(daqShm.daqlock);
    if(daqShm.evr) {
        callbackRequest(&daqShm.daqcb);
        fprintf(stderr, "Starting DAQ SHM writer for segment %d\n", daqShm.segid);
    }
    epicsMutexUnlock(daqShm.daqlock);
}

void time2daq(const char* evrname, int segid, int event)
{
    try {
        if(event==0)
            event = MRF_EVENT_TS_COUNTER_RST;
        else if(event<=0 || event >255) {
            fprintf(stderr, "Invalid event # %d\n", event);
            return;
        }
        if(segid<0 || segid>4) {
            fprintf(stderr, "Invalid segment ID %d\n", segid);
            return;
        }
        mrf::Object *obj = mrf::Object::getObject(evrname);
        if(!obj) {
            fprintf(stderr, "Unknown EVR: %s\n", evrname);
            return;
        }

        EVR *evr = dynamic_cast<EVR*>(obj);
        if(!evr) {
            fprintf(stderr, "\"%s\" is not an EVR\n", evrname);
            return;
        }

        epicsThreadOnce(&daqonce, &daqshminit, 0);

        epicsMutexMustLock(daqShm.daqlock);

        if(daqShm.evr) {
            epicsMutexUnlock(daqShm.daqlock);
            fprintf(stderr, "daqShm already initialized.\n");
            return;
        }

        daqShm.event = event;
        daqShm.evr = evr;
        daqShm.segid = segid;

        epicsMutexUnlock(daqShm.daqlock);
    } catch(std::exception& e) {
        fprintf(stderr, "Error: %s\n", e.what());
    }
}

static const iocshArg time2daqArg0 = { "evr name",iocshArgString};
static const iocshArg time2daqArg1 = { "DAQ segment id",iocshArgInt};
static const iocshArg time2daqArg2 = { "Event code",iocshArgInt};
static const iocshArg * const time2daqArgs[3] =
{&time2daqArg0,&time2daqArg1,&time2daqArg2};
static const iocshFuncDef time2daqFuncDef =
    {"time2daq",3,time2daqArgs};
static void time2daqCallFunc(const iocshArgBuf *args)
{
    time2daq(args[0].sval,args[1].ival,args[2].ival);
}

// static long init_record(dbCommon*) { return 0; }

// static long get_ioint_info(int /*cmd*/, dbCommon */*pRec*/, IOSCANPVT *ppvt)
// {
//     *ppvt = daqShm.lastUpdate;
//     return 0;
// }

// static long read_ok(longinRecord* prec)
// {
//     epicsMutexMustLock(daqShm.ntplock);
//     prec->val = daqShm.numOk;
//     epicsMutexUnlock(daqShm.ntplock);
//     return 0;
// }

// static long read_fail(longinRecord* prec)
// {
//     epicsMutexMustLock(daqShm.ntplock);
//     prec->val = daqShm.numFail;
//     epicsMutexUnlock(daqShm.ntplock);
//     return 0;
// }

// static long read_delta(aiRecord* prec)
// {
//     epicsMutexMustLock(daqShm.ntplock);
//     double val = 0.0;
//     if(daqShm.lastValid)
//         val = epicsTimeDiffInSeconds(&daqShm.lastStamp, &daqShm.lastRx);
//     else
//         recGblSetSevr(prec, READ_ALARM, INVALID_ALARM);
//     if(prec->tse==epicsTimeEventDeviceTime) {
//         prec->time = daqShm.lastStamp;
//     }
//     epicsMutexUnlock(daqShm.ntplock);

//     if(prec->linr==menuConvertLINEAR){
//         val-=prec->eoff;
//         if(prec->eslo!=0)
//             val/=prec->eslo;
//     }
//     val-=prec->aoff;
//     if(prec->aslo!=0)
//         val/=prec->aslo;
//     prec->val = val;
//     prec->udf = !isfinite(val);

//     return 2;
// }

static void daqShmReport(int)
{
    epicsMutexMustLock(daqShm.daqlock);
    EVR *evr=daqShm.evr;
    unsigned int ok=daqShm.numOk,
                 fail=daqShm.numFail;
    epicsMutexUnlock(daqShm.daqlock);

    if(evr) {
        printf("Driver is active\n ok#: %u\n fail#: %u\n", ok, fail);
    } else {
        printf("Driver is not active\n");
    }
}

static void daqShmInit()
{
    // scanIoInit(&daqShm.lastUpdate);
}

static void daqShmRegister()
{
    initHookRegister(&daqshmhooks);
    iocshRegister(&time2daqFuncDef,&time2daqCallFunc);
}

// typedef struct {
//     dset common;
//     DEVSUPFUN read_fn;
//     DEVSUPFUN lin_convert;
// } commonset;

// static commonset devNtpShmLiOk = {
//     {6, NULL, NULL, (DEVSUPFUN)&init_record, (DEVSUPFUN)&get_ioint_info},
//     (DEVSUPFUN)&read_ok,
//     NULL
// };

// static commonset devNtpShmLiFail = {
//     {6, NULL, NULL, (DEVSUPFUN)&init_record, (DEVSUPFUN)&get_ioint_info},
//     (DEVSUPFUN)&read_fail,
//     NULL
// };

// static commonset devNtpShmAiDelta = {
//     {6, NULL, NULL, (DEVSUPFUN)&init_record, (DEVSUPFUN)&get_ioint_info},
//     (DEVSUPFUN)&read_delta,
//     NULL
// };

static drvet daqShared = {
    2,
    (DRVSUPFUN)&daqShmReport,
    (DRVSUPFUN)&daqShmInit,
};

#include <epicsExport.h>

extern "C"{
 epicsExportAddress(drvet, daqShared);
//  epicsExportAddress(dset, devNtpShmLiOk);
//  epicsExportAddress(dset, devNtpShmLiFail);
//  epicsExportAddress(dset, devNtpShmAiDelta);
 epicsExportRegistrar(daqShmRegister);
}
