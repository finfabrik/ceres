#include <time.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <jni.h>
#include "clock.h"


#define SECONDS      1000000000
#define NANOSECONDS  1

JNIEXPORT jlong JNICALL Java_com_blokaly_ceres_system_Clock_currentTimeInNanos
  (JNIEnv *env, jobject thisObj) {

      struct timespec t;
      int r;
      // Implemented as vDSO on modern kernels
      r = clock_gettime(CLOCK_REALTIME, &t);
      uint64_t nowNanos = (uint64_t) t.tv_sec * SECONDS + t.tv_nsec * NANOSECONDS;

      if (r < 0) {
              fprintf(stderr ," Failed, clock_gettime(CLOCK_REALTIME) returned %i exiting \n", r);
              exit(1);
      }
      return (jlong) nowNanos;
}